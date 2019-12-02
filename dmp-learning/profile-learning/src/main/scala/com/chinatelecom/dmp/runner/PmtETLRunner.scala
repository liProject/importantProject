package com.chinatelecom.dmp.runner

import com.chinatelecom.dmp.config.AppConfigHelper
import com.chinatelecom.dmp.etl.IpProcessor
import com.chinatelecom.dmp.untils.SparkSessionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

object PmtETLRunner {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSessionUtils.getSparkSession(this.getClass.getSimpleName.stripSuffix("$"))
    import com.chinatelecom.dmp.untils.KuduUtil._

    val resourceDF: DataFrame = spark
      .read
      .json(AppConfigHelper.AD_DATA_PATH)

    val etlDF: DataFrame = IpProcessor.processor(resourceDF)

    etlDF.saveAsKuduTable(AppConfigHelper.AD_MAIN_TABLE_NAME)

    spark.stop()
  }
}
