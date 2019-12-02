package com.chinatelecom.dmp.tags

import com.chinatelecom.dmp.bean.IdsWithTags
import com.chinatelecom.dmp.config.AppConfigHelper
import com.chinatelecom.dmp.process.Processor
import com.chinatelecom.dmp.untils.TagUtils
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.mutable

object HistoryTagsProcessor extends Processor {
  private val TAG_COEFFICIENT: Double = AppConfigHelper.TAG_COEFF

  override def processor(df: DataFrame): DataFrame = {
    val spark: SparkSession = df.sparkSession
    import spark.implicits._
    df
      .as[IdsWithTags]
      .mapPartitions { iter =>
        iter.map { case IdsWithTags(mainId, ids, tags) =>
          val tagMap: Map[String, Double] = TagUtils.tagStrToMap(tags)
          val newTagsMap: mutable.Map[String, Double] = mutable.Map[String, Double]()
          for (tuple <- tagMap) {
            newTagsMap += tuple._1 -> tuple._2 * TAG_COEFFICIENT
          }
          val tagStr: String = TagUtils.tagMapToStr(newTagsMap.toMap)
          IdsWithTags(mainId,ids,tagStr)
        }
      }
      .toDF()
  }
}
