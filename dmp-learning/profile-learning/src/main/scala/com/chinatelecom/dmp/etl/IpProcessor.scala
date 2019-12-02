package com.chinatelecom.dmp.etl

import com.chinatelecom.dmp.bean.IpRegion
import com.chinatelecom.dmp.config.AppConfigHelper
import com.chinatelecom.dmp.process.Processor
import com.chinatelecom.dmp.untils.IpUtil
import com.maxmind.geoip.LookupService
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.lionsoul.ip2region.{DbConfig, DbSearcher}

object IpProcessor extends Processor{
  override def processor(df: DataFrame):DataFrame = {
    val spark: SparkSession = df.sparkSession
    val newRDD: RDD[Row] = df.rdd.mapPartitions { rows =>
      val ipUtil = new IpUtil
      val dbSearcher: DbSearcher = new DbSearcher(new DbConfig(), AppConfigHelper.IPS_DATA_REGION_PATH)
      val lookupService: LookupService = new LookupService(AppConfigHelper.IPS_DATA_GEO_PATH)
      rows.map { row =>
        val ip: String = row.getAs[String]("ip")
        val region: IpRegion = ipUtil.transfromRegion(ip,dbSearcher,lookupService)
        val newseq: Seq[Any] = row.toSeq :+ region.province :+
        region.city :+
        region.longitude :+
        region.latitude :+
        region.geoHash
        Row.fromSeq(newseq)
      }
    }
    val schema= df.schema
      .add("province", StringType, nullable = true)
      .add("city", StringType, nullable = true)
      .add("longitude", DoubleType, nullable = true)
      .add("latitude", DoubleType, nullable = true)
      .add("geoHash", StringType, nullable = true)
    spark.createDataFrame(newRDD,schema).drop("provincename","cityname","lang","lat")
  }
}
