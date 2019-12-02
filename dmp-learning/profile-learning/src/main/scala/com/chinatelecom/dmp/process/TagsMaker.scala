package com.chinatelecom.dmp.process

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row}

trait TagsMaker extends Logging{
  // 每行数据Row，产生对应的标签
  def make(row: Row, dic: Map[String, String] = null): Map[String, Double]
}
