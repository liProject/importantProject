package com.chinatelecom.dmp.area

import com.chinatelecom.dmp.config.AppConfigHelper
import com.chinatelecom.dmp.process.Processor
import com.chinatelecom.dmp.untils.HttpUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction

object AreaProcessor extends Processor{
  override def processor(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._
    import org.apache.spark.sql.functions._

    val getArea: UserDefinedFunction = udf(HttpUtils.loadGaode2Area _)

    import com.chinatelecom.dmp.untils.KuduUtil._
    
    val currentDF: DataFrame = df
      .filter($"longitude".gt(74) &&
        $"longitude".lt(136) &&
        $"latitude".gt(3) &&
        $"latitude".lt(54))
      .groupBy($"geoHash")
      .agg(
        round(avg($"longitude"),6).as("avg_longitude"),
        round(avg($"latitude"),6)as("avg_latitude")
      )
      .select(
        $"geoHash",
        $"avg_longitude",
        $"avg_latitude"
      )

    val resourceDF: Option[DataFrame] = df.sparkSession.readKudu(AppConfigHelper.BUSINESS_AREAS_TABLE_NAME)

    if (resourceDF.isDefined){
      resourceDF
        .get
        .join(
          currentDF,
          $"geo_Hash" === $"geoHash",
          "right"
        )
        .filter($"geo_Hash".isNull)
        .select(
          $"geoHash".as("geo_Hash"),
          getArea($"avg_longitude",$"avg_latitude").as("area")
        )

    }else{
      currentDF
        .select(
          $"geoHash".as("geo_Hash"),
          getArea($"avg_longitude",$"avg_latitude").as("area")
        )
//      throw new RuntimeException(s"没有在kudu查到${AppConfigHelper.BUSINESS_AREAS_TABLE_NAME}")
    }
  }
}
