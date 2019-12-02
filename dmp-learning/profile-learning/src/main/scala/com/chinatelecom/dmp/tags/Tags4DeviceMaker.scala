package com.chinatelecom.dmp.tags

import com.chinatelecom.dmp.process.TagsMaker
import org.apache.spark.sql.Row

import scala.collection.mutable

object Tags4DeviceMaker extends TagsMaker {
  override def make(row: Row, dic: Map[String, String]): Map[String, Double] = {
    val client: Long = row.getAs[Long]("client")
    val netWorkMannerName: String = row.getAs[String]("networkmannername")
    val ispname: String = row.getAs[String]("ispname")
    val deviceMap: mutable.Map[String, Double] = mutable.Map[String, Double]()
    deviceMap.put(s"DC@$client", 1.0)
    deviceMap.put(s"DC@$netWorkMannerName", 1.0)
    deviceMap.put(s"DC@$ispname", 1.0)
    deviceMap.toMap
  }
}
