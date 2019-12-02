package com.chinatelecom.dmp.tags

import com.chinatelecom.dmp.process.TagsMaker
import org.apache.spark.sql.Row

import scala.collection.mutable

object Tags4KeyWordsMaker extends TagsMaker {
  override def make(row: Row, dic: Map[String, String]): Map[String, Double] = {
    val keyStr: String = row.getAs[String]("keywords")
    val keyArr: Array[String] = keyStr.split(",")
    val keyMap: mutable.Map[String, Double] = mutable.Map[String, Double]()
    for (keyWord <- keyArr) {
      keyMap.put(s"KW@$keyWord", 1.0)
    }
    keyMap.toMap
  }
}
