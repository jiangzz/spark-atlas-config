package com.jd

import com.jd.commons.AtlasParserInterface
import org.apache.spark.sql.SparkSessionExtensions

class AtlasSparkSessionExtensions extends (SparkSessionExtensions => Unit) {
  def apply(e: SparkSessionExtensions): Unit = {
    e.injectParser(AtlasParserInterface)
  }
}