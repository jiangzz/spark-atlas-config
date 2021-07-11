package com.jd

import java.io.FileInputStream
import java.util.Properties

import com.jd.client.AtlasClient
import com.jd.commons.SQLQueryContext
import com.jd.parser.DataWritingCommandHarvester
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.v2.WriteToDataSourceV2Exec
import org.apache.spark.sql.execution.{LeafExecNode, QueryExecution, SparkPlan, UnionExec}
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, DataWritingCommand, DataWritingCommandExec, ExecutedCommandExec, RunnableCommand}
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveTable}

import scala.collection.mutable.Set

class AtlasSparkSQLTracker extends QueryExecutionListener with Logging{
  var client:AtlasClient=AtlasClient("spark-hook.properties")
  private val commands:Set[DataWritingCommand]= Set[DataWritingCommand]()
  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    logInfo("==onSuccess==\t"+Thread.currentThread().getName+"\t"+Thread.currentThread().getId +"\t"+funcName +"\t"+SQLQueryContext.get())

    val commandHarvester = new DataWritingCommandHarvester(client)
    //获取所有的节点计划类型
    var planNodes: Seq[SparkPlan] = qe.sparkPlan.collect {
      case p: UnionExec => p.children
      case p: DataWritingCommandExec => Seq(p)
      case p: WriteToDataSourceV2Exec => Seq(p)
      case p: LeafExecNode => Seq(p)
    }.flatten

    val dataWritingCommands:Seq[DataWritingCommand] = planNodes.collect({ case DataWritingCommandExec(cmd, child) => cmd })
    logInfo("dataWritingCommands："+dataWritingCommands.size)


    if(!dataWritingCommands.isEmpty){
      for (cmd <- dataWritingCommands) {
        if(!commands(cmd)){
          commands.add(cmd)
          cmd match {
            case CreateHiveTableAsSelectCommand(tableDesc,query,outputColumnNames,mode)=>{
              logInfo("=========CreateHiveTableAsSelectCommand=================")
              commandHarvester.harvesterCreateHiveTableAsSelectCommand(cmd.asInstanceOf[CreateHiveTableAsSelectCommand]);
            }
            //处理 Insert Into hive table
            case InsertIntoHiveTable(table,partition,query,overwrite,ifPartitionNotExists,outputColumnNames)=>{
              logInfo("=========InsertIntoHiveTable=================")
              commandHarvester.harvesterInsertIntoHiveTable(cmd.asInstanceOf[InsertIntoHiveTable]);
            }
            //saveAsTable追踪 saveAsTable
            case CreateDataSourceTableAsSelectCommand(table,mode,query,outputColumnNames)=>{
              logInfo("=========CreateDataSourceTableAsSelectCommand=================")
              commandHarvester.harvesterCreateDataSourceTableAsSelectCommand(cmd.asInstanceOf[CreateDataSourceTableAsSelectCommand]);
            }
            case _ => logWarning("目前还未支持处理DataWritingCommand:"+ cmd)
          }
        }else{
          logInfo("已经处理过该Command，不在处理！")
        }
      }
    }
  }
  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
}
