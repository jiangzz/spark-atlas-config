package com.jd

import org.apache.spark.sql.SparkSession

object SparkSQLApplication {
  def main(args: Array[String]): Unit = {
    //配置spark
    val spark = SparkSession.builder()
      .appName("Spark Hive Example")
      .master("yarn")
      .config("hive.metastore.uris", "thrift://CentOS:9083")
      .enableHiveSupport() //启动hive支持
      .getOrCreate()

    spark.sql("use jiangzz")
    spark.sql("drop table if exists t_results").na.fill(0.0).show()

    spark.close()

  }
}
