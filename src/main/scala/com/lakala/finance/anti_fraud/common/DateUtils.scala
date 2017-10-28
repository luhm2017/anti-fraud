package com.lakala.finance.anti_fraud.common

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
  * Created by longxiaolei on 2017/7/18.
  */
object DateUtils {
  private val dayFormat = new SimpleDateFormat("yyyy-MM-dd")

  def getTodayDayStr(): String = {
    val date: Date = new Date
    dayFormat.format(date)
  }

  def getTodayInfo(): (Int,Int,Int) = {
    val cal: Calendar = Calendar.getInstance()
    cal.setTime(new Date)
    (cal.get(Calendar.YEAR),(cal.get(Calendar.MONTH)+1),cal.get(Calendar.DAY_OF_MONTH))
  }



  def main(args: Array[String]): Unit = {
    println(getTodayInfo())
  }
}
