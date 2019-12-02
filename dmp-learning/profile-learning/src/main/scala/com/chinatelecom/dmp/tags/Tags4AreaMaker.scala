package com.chinatelecom.dmp.tags

import com.chinatelecom.dmp.process.TagsMaker
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable

object Tags4AreaMaker extends TagsMaker {
  override def make(row: Row, dic: Map[String, String]): Map[String, Double] = {
    val geoHash: String = row.getAs[String]("geoHash")
    val areaStr: String = dic.get(geoHash) match {
      case Some(area) => area
      case None => return Map()
    }
    val areaArr: Array[String] = areaStr.split(",")
    val areaMap: mutable.Map[String, Double] = mutable.Map[String, Double]()
    for (area <- areaArr) {
      areaMap.put(s"BA@$area", 1.0)
    }
    areaMap.toMap
  }
}
