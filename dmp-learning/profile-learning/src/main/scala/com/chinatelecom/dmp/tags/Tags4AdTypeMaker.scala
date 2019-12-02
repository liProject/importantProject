package com.chinatelecom.dmp.tags

import com.chinatelecom.dmp.process.TagsMaker
import org.apache.spark.sql.{DataFrame, Row}

/**
  * AD@
  * (AD@banner->1.0)
  */
object Tags4AdTypeMaker extends TagsMaker{
  override def make(row: Row, dic: Map[String, String]): Map[String, Double] = {
    val adtype: String = row.getAs[String]("adspacetypename")
    Map(s"AD@$adtype" -> 1.0)
  }
}
