package spark.conf_spark

import java.util
import java.util.Map

import com.typesafe.config.{Config, ConfigFactory, ConfigValue}
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

class GlobalConfigUtil(builder: SparkSession.Builder) {

  def setConfig(): SparkSession.Builder ={
    val config: Config = ConfigFactory.load("spark.conf")
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

  implicit def getConfig(builder: SparkSession.Builder)={
    new GlobalConfigUtil(builder)
  }
}

