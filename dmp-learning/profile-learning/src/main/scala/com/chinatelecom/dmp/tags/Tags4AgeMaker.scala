package com.chinatelecom.dmp.tags

import com.chinatelecom.dmp.process.TagsMaker
import org.apache.spark.sql.{DataFrame, Row}

object Tags4AgeMaker extends TagsMaker{
  override def make(row: Row, dic: Map[String, String]): Map[String, Double] = {
    val age: String = row.getAs[String]("age")
    Map(s"AGE@$age" -> 1.0)
  }
}
