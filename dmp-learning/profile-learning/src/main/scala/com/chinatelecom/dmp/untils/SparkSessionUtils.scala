package com.chinatelecom.dmp.untils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSessionUtils {
  def getSparkSession(bean:String)={
    import com.chinatelecom.dmp.config.GlobalConfigUtil._
    val sparkConf: SparkConf = new SparkConf()
      .setAppName(bean)

    val sparkSession: SparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .setConfig()
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("WARN")

    sparkSession
  }
}
