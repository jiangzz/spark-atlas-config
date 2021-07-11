package com.jd.client
import java.util.{Collections, Properties}

import org.apache.atlas.model.instance.AtlasEntity
import org.apache.atlas.model.notification.HookNotification

class KafkaAtlasClient(props:Properties) extends AtlasHook(props:Properties) with AtlasClient {
  override def publishMessages(ext: AtlasEntity.AtlasEntitiesWithExtInfo): Unit = {
    val notification = new HookNotification.EntityCreateRequestV2("Spark Hook 1.0", ext)
    notifyEntities(Collections.singletonList(notification),null)
   }

  override def getClusterNamespace(): String = props.getProperty("atlas.cluster.name","5k")
}
