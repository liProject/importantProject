package com.chinatelecom.dmp.untils

import java.util.{Calendar, Date}

import org.apache.commons.lang3.time.FastDateFormat

object DateUtil {
  def getTodayDate() = {
    val date = new Date()
    FastDateFormat.getInstance("yyyyMMdd").format(date)
  }

  def getYesterdayDate() = {
    val calendar: Calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE, -1)
    FastDateFormat.getInstance("yyyyMMdd").format(calendar)
  }
}
