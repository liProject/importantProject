package com.chinacom.dmp.untils

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

object DateUtil {
  def getTodayDate() ={
    val date = new Date()
    FastDateFormat.getInstance("yyyyMMdd").format(date)
  }
}
