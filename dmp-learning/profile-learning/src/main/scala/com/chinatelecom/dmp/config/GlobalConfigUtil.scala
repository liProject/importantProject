package com.chinatelecom.dmp.config

import java.util
import java.util.Map

import com.typesafe.config.{Config, ConfigFactory, ConfigValue}
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._

/**
 * @Author li
 * @Description 利用隐式函数动态的注册配置参数
 * @Date 20:43 2019/11/22
 * @Param
 * @return
 **/
class GlobalConfigUtil(builder: SparkSession.Builder) {

  private lazy val config: Config = ConfigFactory.load("spark.conf")
  def setConfig(): SparkSession.Builder ={
    val confEntry: util.Set[Map.Entry[String, ConfigValue]] = config.entrySet()
    for (configMap <- confEntry.asScala){
      val configValue: ConfigValue = configMap.getValue
      if (null != configValue.origin().resource()){
        builder.config(configMap.getKey,configValue.unwrapped().toString)
      }
    }
    builder
  }
}

object GlobalConfigUtil{

  implicit def setConfig(builder: SparkSession.Builder)={
    new GlobalConfigUtil(builder)
  }
}