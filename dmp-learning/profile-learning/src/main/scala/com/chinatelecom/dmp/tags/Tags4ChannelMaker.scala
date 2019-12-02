package com.chinatelecom.dmp.tags

import com.chinatelecom.dmp.process.TagsMaker
import org.apache.spark.sql.Row

object Tags4ChannelMaker extends TagsMaker{
  override def make(row: Row, dic: Map[String, String]): Map[String, Double] = {
    val channelid: String = row.getAs[String]("channelid")
    Map(s"CH@$channelid" -> 1.0)
  }
}
