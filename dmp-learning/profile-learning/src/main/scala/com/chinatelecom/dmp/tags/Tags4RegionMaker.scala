package com.chinatelecom.dmp.tags

import com.chinatelecom.dmp.process.TagsMaker
import org.apache.spark.sql.Row

import scala.collection.mutable

object Tags4RegionMaker extends TagsMaker {
  override def make(row: Row, dic: Map[String, String]): Map[String, Double] = {
    val province: String = row.getAs[String]("province")
    val city: String = row.getAs[String]("city")
    val regionMap: mutable.Map[String, Double] = mutable.Map[String, Double]()
    regionMap.put(s"PN@$province", 1.0)
    regionMap.put(s"CT@$city", 1.0)
    regionMap.toMap
  }
}