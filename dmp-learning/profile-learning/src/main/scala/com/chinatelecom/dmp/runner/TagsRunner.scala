package com.chinatelecom.dmp.runner

import com.chinatelecom.dmp.config.AppConfigHelper
import com.chinatelecom.dmp.tags.{HistoryTagsProcessor, MakeTagsProcessor, MergeTagsProcessor}
import com.chinatelecom.dmp.untils.{DateUtil, SparkSessionUtils}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

object TagsRunner extends Logging{

  private val TodayTAGSNAME: String = AppConfigHelper.TAGS_TABLE_NAME_PREFIX + DateUtil.getTodayDate()
  private val YESTERDAYTAGSNAME: String = AppConfigHelper.TAGS_TABLE_NAME_PREFIX + DateUtil.getYesterdayDate()

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSessionUtils.getSparkSession(this.getClass.getSimpleName.stripSuffix("$"))

    import spark.implicits._
    import com.chinatelecom.dmp.untils.KuduUtil._

    val reourceOption: Option[DataFrame] = spark.readKudu(AppConfigHelper.AD_MAIN_TABLE_NAME)

    val kudu: DataFrame = reourceOption match {
      case Some(kuduDF) => kuduDF
      case None => throw new RuntimeException(s"${AppConfigHelper.AD_MAIN_TABLE_NAME}表不存在")
    }

    val tagDF: DataFrame = MakeTagsProcessor.processor(kudu)

    val oldOption: Option[DataFrame] = kudu.readKudu(YESTERDAYTAGSNAME)

    val allTagDF: DataFrame = oldOption match {
      case Some(yesDF) => {
        tagDF.unionAll(HistoryTagsProcessor.processor(yesDF))
      }
      case None => {
        logWarning("没有历史标签表数据")
        tagDF
      }
    }

    val userTags: DataFrame = MergeTagsProcessor.processor(allTagDF)

    userTags.createKuduTable(TodayTAGSNAME,userTags.schema,Seq("mainId"),3,isDelete = true)

    userTags.saveAsKuduTable(TodayTAGSNAME)




    spark.stop()

  }
}
