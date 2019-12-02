package com.chinatelecom.dmp.runner

import com.chinatelecom.dmp.area.AreaProcessor
import com.chinatelecom.dmp.config.AppConfigHelper
import com.chinatelecom.dmp.untils.SparkSessionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

object BusinessAreasRunner {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSessionUtils.getSparkSession(this.getClass.getSimpleName.stripSuffix("$"))
    import com.chinatelecom.dmp.untils.KuduUtil._

    val resourceDF: Option[DataFrame] = spark.readKudu(AppConfigHelper.AD_MAIN_TABLE_NAME)

    if (resourceDF.isEmpty){
      throw new RuntimeException(s"kudu中没有${AppConfigHelper.AD_MAIN_TABLE_NAME}表")
    }

    val areaDF: DataFrame = AreaProcessor.processor(resourceDF.get)

    spark.createKuduTable(AppConfigHelper.BUSINESS_AREAS_TABLE_NAME,areaDF.schema,Seq("geo_Hash"),3,isDelete = false)

    areaDF.saveAsKuduTable(AppConfigHelper.BUSINESS_AREAS_TABLE_NAME)

    spark.stop()
  }
}
