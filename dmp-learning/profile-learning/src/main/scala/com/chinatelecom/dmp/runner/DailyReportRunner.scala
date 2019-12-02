package com.chinatelecom.dmp.runner

import com.chinatelecom.dmp.config.AppConfigHelper
import com.chinatelecom.dmp.report.ReportAdsProcessor
import com.chinatelecom.dmp.untils.SparkSessionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

object DailyReportRunner {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSessionUtils.getSparkSession(this.getClass.getSimpleName.stripSuffix("$"))
    import com.chinatelecom.dmp.untils.KuduUtil._

    val optionDF: Option[DataFrame] = spark.readKudu(AppConfigHelper.AD_MAIN_TABLE_NAME)

    val kuduDF: DataFrame = optionDF match {
      case someDF: Some[DataFrame] => someDF.get
      case none => throw new RuntimeException(s"${AppConfigHelper.AD_MAIN_TABLE_NAME}表不存在")
    }

    kuduDF.persist(StorageLevel.MEMORY_AND_DISK)

    ReportAdsProcessor.processData(kuduDF, AppConfigHelper.REPORT_ADS_APP_TABLE_NAME, Seq("report_date", "appid", "appname"))
    ReportAdsProcessor.processData(kuduDF, AppConfigHelper.REPORT_ADS_CHANNEL_TABLE_NAME, Seq("report_date", "channelid"))
    ReportAdsProcessor.processData(kuduDF, AppConfigHelper.REPORT_ADS_DEVICE_TABLE_NAME, Seq("report_date", "client", "device"))
    ReportAdsProcessor.processData(kuduDF, AppConfigHelper.REPORT_ADS_ISP_TABLE_NAME, Seq("report_date", "ispid", "ispname"))
    ReportAdsProcessor.processData(kuduDF, AppConfigHelper.REPORT_ADS_NETWORK_TABLE_NAME, Seq("report_date", "networkmannerid", "networkmannername"))
    ReportAdsProcessor.processData(kuduDF, AppConfigHelper.REPORT_ADS_REGION_TABLE_NAME, Seq("report_date", "province", "city"))

    //    val reportConf: Map[String, Seq[String]] = Map[String, Seq[String]]()
    //    reportConf + AppConfigHelper.REPORT_ADS_APP_TABLE_NAME -> Seq("report_date", "appid", "appname")
    //    reportConf + AppConfigHelper.REPORT_ADS_CHANNEL_TABLE_NAME -> Seq("report_date", "channelid")
    //    reportConf + AppConfigHelper.REPORT_ADS_DEVICE_TABLE_NAME -> Seq("report_date", "client", "device")
    //    reportConf + AppConfigHelper.REPORT_ADS_ISP_TABLE_NAME -> Seq("report_date", "ispid", "ispname")
    //    reportConf + AppConfigHelper.REPORT_ADS_NETWORK_TABLE_NAME -> Seq("report_date", "networkmannerid", "networkmannername")
    //    reportConf + AppConfigHelper.REPORT_ADS_REGION_TABLE_NAME -> Seq("report_date", "province", "city")


    //    val confRDD: RDD[(String, Seq[String])] = spark.sparkContext
    //      .parallelize(reportConf.toSeq)
    //
    //    confRDD
    //      .foreachPartition {iter =>
    //        iter.foreach{config =>
    //          ReportAdsProcessor.processData(kuduDF, config._1, config._2)
    //        }
    //      }


    kuduDF.unpersist()
    spark.stop()
  }
}
