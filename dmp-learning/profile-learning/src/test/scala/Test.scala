import java.util
import java.util.Map

import com.typesafe.config.{Config, ConfigFactory, ConfigValue}
import scala.collection.JavaConverters._

object Test {
  def main(args: Array[String]): Unit = {
    val tableKeyField = Seq("report_date", "appid", "appname")

    val groupField: Seq[String] = tableKeyField.drop(1)

    println(groupField)
  }
}
