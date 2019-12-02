package com.chinatelecom.dmp.tags

import com.chinatelecom.dmp.process.TagsMaker
import org.apache.spark.sql.{DataFrame, Row}

object Tags4AppMaker extends TagsMaker{
  override def make(row: Row, dic: Map[String, String]): Map[String, Double] = {
    val app: String = row.getAs[String]("appname")
    Map(s"APP@$app" -> 1.0)
  }
}
