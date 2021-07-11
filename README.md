# spark-atlas-config
## 创建配置文件
```shell
[root@CentOS ~]# cat spark-hook.properties 
atlas.cluster.name=5k
atlas.hook.topic=ATLAS_HOOK_V2
atlas.kafka.zookeeper.connect=CentOS:2181
atlas.kafka.bootstrap.servers=CentOS:9092
```
## 执行以下脚本
```shell
[root@CentOS spark-2.4.5]# ./bin/spark-submit  --master yarn  --deploy-mode  cluster  --class com.jd.SparkSQLApplication  --conf spark.sql.queryExecutionListeners=com.jd.AtlasSparkSQLTracker  --conf spark.sql.extensions=com.jd.AtlasSparkSessionExtensions  --jars /root/spark-atlas-hook-1.0-SNAPSHOT.jar --files file:///root/spark-hook.properties  /root/spark-sql-1.0-SNAPSHOT.jar 
```
