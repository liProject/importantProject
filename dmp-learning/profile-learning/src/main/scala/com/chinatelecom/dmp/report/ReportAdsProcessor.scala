package com.chinatelecom.dmp.report

import com.chinatelecom.dmp.process.ReportProcessor
import org.apache.spark.sql.{DataFrame, SparkSession}

object ReportAdsProcessor extends ReportProcessor {

  override def realProcessData(df: DataFrame,groupField:Seq[String]): DataFrame = {
    val spark: SparkSession = df.sparkSession

    df.createOrReplaceTempView("view_tmp_ods")

    val seqDF: DataFrame = spark.sql(
      ReportSQLConstant.reportAdsKpiWithSQL("view_tmp_ods", groupField)
    )

    logInfo("sql运行完毕")

    seqDF
  }


}
