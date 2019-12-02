package com.chinatelecom.dmp.tags

import com.chinatelecom.dmp.bean.IdsWithTags
import com.chinatelecom.dmp.config.AppConfigHelper
import com.chinatelecom.dmp.process.Processor
import com.chinatelecom.dmp.untils.TagUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable

object MakeTagsProcessor extends Processor {
  override def processor(df: DataFrame): DataFrame = {

    val spark: SparkSession = df.sparkSession

    import spark.implicits._
    import com.chinatelecom.dmp.untils.KuduUtil._

    val optionDF: Option[DataFrame] = spark.readKudu(AppConfigHelper.BUSINESS_AREAS_TABLE_NAME)

    val areaDF: DataFrame = optionDF match {
      case Some(df) => df
      case None => throw new RuntimeException(s"kudu中没有${AppConfigHelper.BUSINESS_AREAS_TABLE_NAME}")
    }

    val areaMap: Map[String, String] = areaDF
      .mapPartitions { iter =>
        iter
          .filter("List()" != _.getAs[String]("area"))
          .map { row =>
            val geoHash: String = row.getAs[String]("geo_Hash")
            val areaStrs: String = row.getAs[String]("area")
            geoHash -> areaStrs
          }
      }
      .collect().toMap

    val areaBroadCast: Broadcast[Map[String, String]] = spark.sparkContext.broadcast(areaMap)

    val tagDS: Dataset[IdsWithTags] = df.mapPartitions { iter =>

      iter.map { row =>
        val mainId: String = row.getAs[String]("uuid").toString
        // a. 构建集合Map对象，用于存放标签数据
        var tagsMap: mutable.Map[String, Double] = mutable.Map[String, Double]()

        // 1). 广告类型标签（Tags4AdTypeMaker）
        tagsMap ++= Tags4AdTypeMaker.make(row)
        // 2). 渠道标签（Tags4ChannelMaker）
        tagsMap ++= Tags4ChannelMaker.make(row)
        // 3). 关键词标签（Tags4KeyWordsMaker)
        tagsMap ++= Tags4KeyWordsMaker.make(row)
        // 4). 省份城市标签（Tags4RegionMaker）
        tagsMap ++= Tags4RegionMaker.make(row)
        // 5). 性别标签（Tags4GenderMaker）
        tagsMap ++= Tags4GenderMaker.make(row)
        // 6). 年龄标签（Tags4AgeMaker）
        tagsMap ++= Tags4AgeMaker.make(row)
        // 7). 商圈标签（Tags4AreaMaker）
        tagsMap ++= Tags4AreaMaker.make(row, areaBroadCast.value)
        // 8). App标签（Tags4AppMaker）
        tagsMap ++= Tags4AppMaker.make(row)
        // 9). 设备标签（Tags4DeviceMaker）
        tagsMap ++= Tags4DeviceMaker.make(row)

        //        val tagsStr: String = tagsMap.toList.sortBy(_._1).mkString(",")

        val idsMap: Map[String, String] = getIds(AppConfigHelper.ID_FIELDS, row)

        IdsWithTags(mainId, TagUtils.idsMapToStr(idsMap), TagUtils.tagMapToStr(tagsMap.toMap))
      }
    }

    tagDS.toDF()
  }

  def getIds(idsNameArr: String, row: Row) = {
    idsNameArr.split(",").map { idsName =>
      idsName -> row.getAs[String](idsName)
    }
      .filter(tuple => StringUtils.isNotBlank(tuple._2))
      .toMap
  }
}
