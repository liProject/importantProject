package com.chinacom.dmp.app

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

object App extends Logging{
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = {

      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))

      import com.chinacom.dmp.config.GlobalConfigUtil._

      SparkSession
        .builder()
        .config(sparkConf)
        .setConfig()
        .getOrCreate()
    }

    /**
      * def encoderFor[A : Encoder]: ExpressionEncoder[A] = implicitly[Encoder[A]] match {
      * case e: ExpressionEncoder[A] =>
      *       e.assertUnresolved()
      * e
      * case _ => sys.error(s"Only expression encoders are supported today")
      * }
      */
    logInfo("info")
    logError("error")
    logWarning("warning")

    Thread.sleep(100000000)

    spark.stop()
  }
}
