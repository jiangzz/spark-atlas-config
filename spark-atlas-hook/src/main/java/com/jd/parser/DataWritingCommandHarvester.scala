package com.jd.parser


import com.jd.client.AtlasClient
import org.apache.atlas.model.instance.{AtlasEntity, AtlasObjectId, AtlasRelatedObjectId}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveTable}
import org.slf4j.{Logger, LoggerFactory}
import java.util
import java.util.Date

import com.jd.commons.NotificationContextHolder
import com.jd.tracker.SQLQueryContext
import org.apache.atlas.`type`.AtlasTypeUtil

import scala.collection.mutable.{HashMap, ListBuffer}


class DataWritingCommandHarvester(client:AtlasClient){
  val log:Logger=LoggerFactory.getLogger(classOf[DataWritingCommandHarvester])
  val namespace=client.getClusterNamespace()
  def harvesterCreateHiveTableAsSelectCommand(cmd:CreateHiveTableAsSelectCommand):Unit={
    val tableName = cmd.tableDesc.identifier.table

    var database:String="null";
    if(cmd.tableDesc.identifier.database.isEmpty){
      val sparkSession = SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession)
      database = sparkSession.get.sessionState.catalog.getCurrentDatabase
    }else{
      database = cmd.tableDesc.identifier.database.get
    }

    val tableRelations = cmd.collectLeaves().collect({ case t: HiveTableRelation => t })
    val outputTable=s"${database}.${tableName}@${namespace}"
    val inputTables = tableRelations.map(table => table.tableMeta).map(t => s"${t.identifier.database.getOrElse("default")}.${t.identifier.table}@${namespace}")
    if(log.isInfoEnabled){
        for (input <- inputTables) {
          log.info(s"表血缘:${input} -> ${outputTable}")
        }
    }

    val tableProcessEntity = harvestTableLineage(outputTable, inputTables,"CREATETABLE_AS_SELECT")
    val columnLineageEntities=ListBuffer[AtlasEntity]()


    val logicalPlan = cmd.query
    if(logicalPlan.isInstanceOf[Project]){
      val tableColumnsMap = new HashMap[Long, String]

      //解析输入表的所有字段信息
      tableRelations.map(table=>(table.tableMeta,table.dataCols,table.partitionCols)).foreach(t=>{
          val identifier = t._1.identifier
          for (elem <- t._2.map(_.asInstanceOf[NamedExpression])) {
            tableColumnsMap.put(elem.exprId.id,s"${identifier.database.getOrElse("default")}.${identifier.table}.${elem.name}@${namespace}".toLowerCase())
          }
          if(!t._3.isEmpty){//处理分区字段
            for (elem <- t._3.map(_.asInstanceOf[NamedExpression])) {
              tableColumnsMap.put(elem.exprId.id,s"${identifier.database.getOrElse("default")}.${identifier.table}.${elem.name}@${namespace}".toLowerCase())
            }
          }
      })

      val project = logicalPlan.asInstanceOf[Project]
      val outputsColumns = project.projectList
      for (elem <- outputsColumns) {
        val expID = elem.exprId.id
        val columnName=s"${database}.${tableName}.${elem.name.toLowerCase}@${namespace}"
        if(tableColumnsMap.contains(expID)){
          columnLineageEntities+=harvestColumnLineage(database,tableName,namespace,elem.name.toLowerCase,columnName,List(tableColumnsMap.get(expID).get),null,tableProcessEntity)
        }else{
          //获取子字段血缘信息
          if(!elem.children.isEmpty){
            var expression = elem.children.collect({ case e: Expression => e }).mkString(",")

            var listColumns=ListBuffer[String]()
            elem.collectLeaves().foreach(e=>{
                val nameExpression = e.asInstanceOf[NamedExpression]
                val colId = nameExpression.exprId.id
                if(tableColumnsMap.contains(colId)){
                  listColumns +=tableColumnsMap.get(colId).get
                }
            })
            if(listColumns.size==1 && ! (expression.contains("(") && expression.contains(")"))){
              expression=null;
            }
            columnLineageEntities+=harvestColumnLineage(database,tableName,namespace,elem.name.toLowerCase,columnName,listColumns,expression,tableProcessEntity)
          }
        }
      }
    }else if(logicalPlan.isInstanceOf[HiveTableRelation]){
      //说明是直接create xxx as select * from
      //解析输入表的所有字段信息
      val tableColumnsMap = new HashMap[String, String]
      tableRelations.map(table=>(table.tableMeta,table.dataCols,table.partitionCols)).foreach(t=>{
        val identifier = t._1.identifier
        for (elem <- t._2.map(_.asInstanceOf[NamedExpression])) {
          tableColumnsMap.put(elem.name,s"${identifier.database.getOrElse("default")}.${identifier.table}.${elem.name}@${namespace}".toLowerCase())
        }
        if(!t._3.isEmpty){//处理分区字段
          for (elem <- t._3.map(_.asInstanceOf[NamedExpression])) {
            tableColumnsMap.put(elem.name,s"${identifier.database.getOrElse("default")}.${identifier.table}.${elem.name}@${namespace}".toLowerCase())
          }
        }
        for (elem <- tableColumnsMap.keySet) {
          columnLineageEntities+=harvestColumnLineage(database, tableName, namespace, elem.toLowerCase,
            s"${database}.${tableName}.${elem.toLowerCase}@${namespace}", List(tableColumnsMap.get(elem).get),null,tableProcessEntity)
        }
      })
    }
    NotificationContextHolder.setMessageKey(outputTable)
    val withExtInfo = new AtlasEntity.AtlasEntitiesWithExtInfo()
    withExtInfo.addEntity(tableProcessEntity)
    columnLineageEntities.foreach(entity=>withExtInfo.addEntity(entity))
    client.publishMessages(withExtInfo)

  }
  def harvesterInsertIntoHiveTable(cmd:InsertIntoHiveTable):Unit={
    val tableName = cmd.table.identifier.table
    var database:String="null";
    if(cmd.table.identifier.database.isEmpty){
      val sparkSession = SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession)
      database = sparkSession.get.sessionState.catalog.getCurrentDatabase
    }else{
      database = cmd.table.identifier.database.get
    }

    val tableRelations = cmd.collectLeaves().collect({ case t: HiveTableRelation => t })
    val outputTable=s"${database}.${tableName}@${namespace}"
    val inputTables = tableRelations.map(table => table.tableMeta).map(t => s"${t.identifier.database.getOrElse("default")}.${t.identifier.table}@${namespace}")
    if(log.isInfoEnabled){
      for (input <- inputTables) {
        log.info(s"表血缘:${input} -> ${outputTable}")
      }
    }
    val tableProcessEntity = harvestTableLineage(outputTable, inputTables,"INSERT_INTO_HIVE_TABLE")
    val columnLineageEntities=ListBuffer[AtlasEntity]()

    val logicalPlan = cmd.query
    if(logicalPlan.isInstanceOf[Project]){
      val tableColumnsMap = new HashMap[Long, String]

      //解析输入表的所有字段信息
      tableRelations.map(table=>(table.tableMeta,table.dataCols,table.partitionCols)).foreach(t=>{
        val identifier = t._1.identifier
        for (elem <- t._2.map(_.asInstanceOf[NamedExpression])) {
          tableColumnsMap.put(elem.exprId.id,s"${identifier.database.getOrElse("default")}.${identifier.table}.${elem.name}@${namespace}".toLowerCase())
        }
        if(!t._3.isEmpty){//处理分区字段
          for (elem <- t._3.map(_.asInstanceOf[NamedExpression])) {
            tableColumnsMap.put(elem.exprId.id,s"${identifier.database.getOrElse("default")}.${identifier.table}.${elem.name}@${namespace}".toLowerCase())
          }
        }
      })

      val project = logicalPlan.asInstanceOf[Project]
      val outputsColumns = project.projectList
      for (elem <- outputsColumns) {
        val expID = elem.exprId.id
        val columnName=s"${database}.${tableName}.${elem.name.toLowerCase}@${namespace}"
        if(tableColumnsMap.contains(expID)){
          columnLineageEntities+=harvestColumnLineage(database,tableName,namespace,elem.name.toLowerCase,columnName,List(tableColumnsMap.get(expID).get),null,tableProcessEntity)
        }else{
          //获取子字段血缘信息
          if(!elem.children.isEmpty){
            var expression = elem.children.collect({ case e: Expression => e }).mkString(",")

            var listColumns=ListBuffer[String]()
            elem.collectLeaves().foreach(e=>{
              val nameExpression = e.asInstanceOf[NamedExpression]
              val colId = nameExpression.exprId.id
              if(tableColumnsMap.contains(colId)){
                listColumns +=tableColumnsMap.get(colId).get
              }
            })
            if(listColumns.size==1 && ! (expression.contains("(") && expression.contains(")"))){
              expression=null;
            }
            columnLineageEntities+=harvestColumnLineage(database,tableName,namespace,elem.name.toLowerCase,columnName,listColumns,expression,tableProcessEntity)
          }
        }
      }
    }else if(logicalPlan.isInstanceOf[HiveTableRelation]){
      //说明是直接create xxx as select * from
      //解析输入表的所有字段信息
      val tableColumnsMap = new HashMap[String, String]
      tableRelations.map(table=>(table.tableMeta,table.dataCols,table.partitionCols)).foreach(t=>{
        val identifier = t._1.identifier
        for (elem <- t._2.map(_.asInstanceOf[NamedExpression])) {
          tableColumnsMap.put(elem.name,s"${identifier.database.getOrElse("default")}.${identifier.table}.${elem.name}@${namespace}".toLowerCase())
        }
        if(!t._3.isEmpty){//处理分区字段
          for (elem <- t._3.map(_.asInstanceOf[NamedExpression])) {
            tableColumnsMap.put(elem.name,s"${identifier.database.getOrElse("default")}.${identifier.table}.${elem.name}@${namespace}".toLowerCase())
          }
        }
        for (elem <- tableColumnsMap.keySet) {
          columnLineageEntities+=harvestColumnLineage(database, tableName, namespace, elem.toLowerCase,
            s"${database}.${tableName}.${elem.toLowerCase}@${namespace}", List(tableColumnsMap.get(elem).get),null,tableProcessEntity)
        }
      })
    }
    NotificationContextHolder.setMessageKey(outputTable)
    val withExtInfo = new AtlasEntity.AtlasEntitiesWithExtInfo()
    withExtInfo.addEntity(tableProcessEntity)
    columnLineageEntities.foreach(entity=>withExtInfo.addEntity(entity))
    client.publishMessages(withExtInfo)
  }
  def harvesterCreateDataSourceTableAsSelectCommand(cmd:CreateDataSourceTableAsSelectCommand):Unit={
    val tableName = cmd.table.identifier.table
    var database:String="null";
    if(cmd.table.identifier.database.isEmpty){
      val sparkSession = SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession)
      database = sparkSession.get.sessionState.catalog.getCurrentDatabase
    }else{
      database = cmd.table.identifier.database.get
    }

    val tableRelations = cmd.collectLeaves().collect({ case t: HiveTableRelation => t })
    val outputTable=s"${database}.${tableName}@${namespace}"
    val inputTables = tableRelations.map(table => table.tableMeta).map(t => s"${t.identifier.database.getOrElse("default")}.${t.identifier.table}@${namespace}")
    if(log.isInfoEnabled){
      for (input <- inputTables) {
        log.info(s"表血缘:${input} -> ${outputTable}")
      }
    }

    val tableProcessEntity = harvestTableLineage(outputTable, inputTables,"CREATETABLE_AS_SELECT")
    val columnLineageEntities=ListBuffer[AtlasEntity]()


    val logicalPlan = cmd.query
    if(logicalPlan.isInstanceOf[Project]){
      val tableColumnsMap = new HashMap[Long, String]

      //解析输入表的所有字段信息
      tableRelations.map(table=>(table.tableMeta,table.dataCols,table.partitionCols)).foreach(t=>{
        val identifier = t._1.identifier
        for (elem <- t._2.map(_.asInstanceOf[NamedExpression])) {
          tableColumnsMap.put(elem.exprId.id,s"${identifier.database.getOrElse("default")}.${identifier.table}.${elem.name}@${namespace}".toLowerCase())
        }
        if(!t._3.isEmpty){//处理分区字段
          for (elem <- t._3.map(_.asInstanceOf[NamedExpression])) {
            tableColumnsMap.put(elem.exprId.id,s"${identifier.database.getOrElse("default")}.${identifier.table}.${elem.name}@${namespace}".toLowerCase())
          }
        }
      })

      val project = logicalPlan.asInstanceOf[Project]
      val outputsColumns = project.projectList
      for (elem <- outputsColumns) {
        val expID = elem.exprId.id
        val columnName=s"${database}.${tableName}.${elem.name.toLowerCase}@${namespace}"
        if(tableColumnsMap.contains(expID)){
          columnLineageEntities+=harvestColumnLineage(database,tableName,namespace,elem.name.toLowerCase,columnName,List(tableColumnsMap.get(expID).get),null,tableProcessEntity)
        }else{
          //获取子字段血缘信息
          if(!elem.children.isEmpty){
            var expression = elem.children.collect({ case e: Expression => e }).mkString(",")

            var listColumns=ListBuffer[String]()
            elem.collectLeaves().foreach(e=>{
              val nameExpression = e.asInstanceOf[NamedExpression]
              val colId = nameExpression.exprId.id
              if(tableColumnsMap.contains(colId)){
                listColumns +=tableColumnsMap.get(colId).get
              }
            })
            if(listColumns.size==1 && ! (expression.contains("(") && expression.contains(")"))){
              expression=null;
            }
            columnLineageEntities+=harvestColumnLineage(database,tableName,namespace,elem.name.toLowerCase,columnName,listColumns,expression,tableProcessEntity)
          }
        }
      }
    }else if(logicalPlan.isInstanceOf[HiveTableRelation]){
      //说明是直接create xxx as select * from
      //解析输入表的所有字段信息
      val tableColumnsMap = new HashMap[String, String]
      tableRelations.map(table=>(table.tableMeta,table.dataCols,table.partitionCols)).foreach(t=>{
        val identifier = t._1.identifier
        for (elem <- t._2.map(_.asInstanceOf[NamedExpression])) {
          tableColumnsMap.put(elem.name,s"${identifier.database.getOrElse("default")}.${identifier.table}.${elem.name}@${namespace}".toLowerCase())
        }
        if(!t._3.isEmpty){//处理分区字段
          for (elem <- t._3.map(_.asInstanceOf[NamedExpression])) {
            tableColumnsMap.put(elem.name,s"${identifier.database.getOrElse("default")}.${identifier.table}.${elem.name}@${namespace}".toLowerCase())
          }
        }
        for (elem <- tableColumnsMap.keySet) {
          columnLineageEntities+=harvestColumnLineage(database, tableName, namespace, elem.toLowerCase,
            s"${database}.${tableName}.${elem.toLowerCase}@${namespace}", List(tableColumnsMap.get(elem).get),null,tableProcessEntity)
        }
      })
    }
    NotificationContextHolder.setMessageKey(outputTable)
    val withExtInfo = new AtlasEntity.AtlasEntitiesWithExtInfo()
    withExtInfo.addEntity(tableProcessEntity)
    columnLineageEntities.foreach(entity=>withExtInfo.addEntity(entity))
    client.publishMessages(withExtInfo)
  }

  def harvestColumnLineage(database:String,table:String,cluster:String,shortColumn:String,resultColumn:String,
                           inputColumns:Seq[String], expression:String,hiveProcess:AtlasEntity):AtlasEntity={

    log.info(s"字段血缘 ${resultColumn} - ${expression} -> ${inputColumns.mkString(",")}")

    val atlasEntity: AtlasEntity = new AtlasEntity("hive_column_lineage")
    //封装输入表信息
    val inputs: util.List[AtlasRelatedObjectId] = new util.ArrayList[AtlasRelatedObjectId]
    inputColumns.foreach(column=>inputs.add(new AtlasRelatedObjectId(new AtlasObjectId("hive_column", "qualifiedName", column), "dataset_process_inputs")))

    //封装输出表信息
    val outputs: util.List[AtlasRelatedObjectId] = new util.ArrayList[AtlasRelatedObjectId]
    outputs.add(new AtlasRelatedObjectId(new AtlasObjectId("hive_column", "qualifiedName", resultColumn), "process_dataset_outputs"))

    atlasEntity.setRelationshipAttribute("inputs", inputs)
    atlasEntity.setRelationshipAttribute("outputs", outputs)

    val qualifiedName=s"${database}.${table}@${cluster}:${shortColumn}"
    atlasEntity.setAttribute("qualifiedName", qualifiedName)
    atlasEntity.setAttribute("name", qualifiedName)
    if(expression==null){
      atlasEntity.setAttribute("depenendencyType", "SIMPLE")
    }else{
      atlasEntity.setAttribute("depenendencyType", "EXPRESSION")
      atlasEntity.setAttribute("expression", expression)
    }
    atlasEntity.setAttribute("startTime", new Date)
    atlasEntity.setAttribute("endTime", new Date)
    atlasEntity.setRelationshipAttribute("query", AtlasTypeUtil.getAtlasRelatedObjectId(hiveProcess,"hive_process_column_lineage"))
    atlasEntity
  }

  private def harvestTableLineage(resultTable:String,inputTables:Seq[String],operType:String):AtlasEntity={

    val atlasEntity: AtlasEntity = new AtlasEntity("hive_process")
    //封装输入表信息
    val inputs: util.List[AtlasRelatedObjectId] = new util.ArrayList[AtlasRelatedObjectId]
    inputTables.foreach(table=>inputs.add(new AtlasRelatedObjectId(new AtlasObjectId("hive_table", "qualifiedName", table), "dataset_process_inputs")))

    //封装输出表信息
    val outputs: util.List[AtlasRelatedObjectId] = new util.ArrayList[AtlasRelatedObjectId]
    outputs.add(new AtlasRelatedObjectId(new AtlasObjectId("hive_table", "qualifiedName", resultTable), "process_dataset_outputs"))

    atlasEntity.setRelationshipAttribute("inputs", inputs)
    atlasEntity.setRelationshipAttribute("outputs", outputs)

    val qualifiedName=s"${resultTable}"
    atlasEntity.setAttribute("qualifiedName", qualifiedName)
    atlasEntity.setAttribute("name", qualifiedName)
    atlasEntity.setAttribute("userName", "Spark")
    atlasEntity.setAttribute("startTime", new Date)
    atlasEntity.setAttribute("endTime", new Date)
    var querySQL = SQLQueryContext.get()
    atlasEntity.setAttribute("operationType", operType)
    atlasEntity.setAttribute("queryId", querySQL)
    atlasEntity.setAttribute("queryText", querySQL)
    atlasEntity.setAttribute("recentQueries", util.Collections.singletonList(querySQL))
    atlasEntity.setAttribute("queryPlan", "Not Supported")
    atlasEntity.setAttribute("clusterName", client.getClusterNamespace())
    atlasEntity
  }
}
