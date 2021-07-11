package com.jd.client

import java.io.FileInputStream
import java.util.Properties

import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo
import org.apache.spark.internal.Logging

trait AtlasClient  extends Logging{
  def publishMessages(ext:AtlasEntitiesWithExtInfo):Unit
  def getClusterNamespace():String
}
object AtlasClient extends Logging {
   def apply(filename:String): AtlasClient ={
     logInfo("创建Atlas Client")
     val properties = new Properties()
     properties.load(new FileInputStream(filename))
     new KafkaAtlasClient(properties)
   }
}
