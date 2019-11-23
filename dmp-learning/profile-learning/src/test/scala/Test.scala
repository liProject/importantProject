import java.util
import java.util.Map

import com.typesafe.config.{Config, ConfigFactory, ConfigValue}
import scala.collection.JavaConverters._

object Test {
  def main(args: Array[String]): Unit = {
    val config: Config = ConfigFactory.load("spark.conf")
    val set: util.Set[Map.Entry[String, ConfigValue]] = config.entrySet()
    for(map <- set.asScala){
      if ((null != map.getValue.origin().resource())){
        println(s"${map.getValue.origin().resource()}==========${map.getKey}++++++++${map.getValue.unwrapped()}")
      }
    }
  }
}
