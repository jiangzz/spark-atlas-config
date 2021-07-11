package com.jd.commons

import org.apache.spark.sql.execution.QueryExecution

case class QueryeExecutionDetail(queryExecution: QueryExecution, sql:Option[String])

