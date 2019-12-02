package com.chinatelecom.dmp.untils

import com.chinatelecom.dmp.config.AppConfigHelper
import okhttp3.{OkHttpClient, Request, Response}
import org.json4s

import scala.collection.mutable.ListBuffer

object HttpUtils {
  private val client: OkHttpClient = new OkHttpClient()

  def getLocaltionInfo(longitude:Double,latitude:Double):Option[String]={
    val url: String = AppConfigHelper.AMAP_URL + s"${longitude},${latitude}"

    try {
      val request: Request = new Request.Builder()
        .url(url)
        .get()
        .build()

      val response: Response = client.newCall(request).execute()

      if (response.isSuccessful) {
        val responseJson: String = response.body().string()

        Some(responseJson)
      } else {
        None
      }
    }catch {
      case e: Exception => e.printStackTrace(); None
    }
  }

  def parseJson(geodeJson:String):String={
    import org.json4s._
    import org.json4s.jackson.JsonMethods._

    val jValueRoot: JValue = parse(geodeJson)

    val jValueAreas: JValue = jValueRoot.\\("businessAreas")

    val children: List[json4s.JValue] = jValueAreas.children

    val businessAreaList = new ListBuffer[String]()

    for (jValue <- children) {
      businessAreaList.append(jValue.\("name").values.toString)
    }
    businessAreaList.mkString(",")
  }

  def loadGaode2Area(longitude:Double,latitude:Double):String ={
    val respJson: Option[String] = getLocaltionInfo(longitude,latitude)

    respJson match {
      case Some(json) => parseJson(json)
      case None => ""
    }
  }

//  def main(args: Array[String]): Unit = {
//    println(loadGaode2Area(104.066711, 30.666702))
//    println(getLocaltionInfo(104.066711, 30.666702))
//
//  }
}
