package spark.conf_spark

import java.sql.Date

import org.apache.commons.lang3.time.FastDateFormat

object DateUtil {
  def getTodayDate() ={
    val date = new Date()
    FastDateFormat.getInstance("yyyyMMdd").format(date)
  }
}
