package com.chinatelecom.dmp.tags

import com.chinatelecom.dmp.process.TagsMaker
import org.apache.spark.sql.Row

object Tags4GenderMaker extends TagsMaker{
  override def make(row: Row, dic: Map[String, String]): Map[String, Double] = {
    val sex: String = row.getAs[String]("sex")
    sex match {
      case "1" => Map(s"GD@男" -> 1.0)
      case "0" => Map(s"GD@女" -> 1.0)
      case _ => logWarning("年龄中出现非法数")
        throw new RuntimeException()
    }
  }
}
