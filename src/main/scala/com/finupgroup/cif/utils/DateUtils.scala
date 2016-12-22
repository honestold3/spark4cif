package com.finupgroup.cif.utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import scala.collection.mutable.ArrayBuffer

/**
*  Created by zcq
*  on 2016-1-5.
*/
object DateUtils {
  val reg = """(\d{4}\-\d{2}\-\d{2} \d{2}):(\d{2}):\d{2}""".r

  /**
   * 获得当前日期
   * @return 返回当天日期
   */
  def getNowTime = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    dateFormat.format(now)
  }

  /**
   * 返回指定字符串对应的unix时间
   * @param str 时间字符串
   * @return unix时间
   */
  def getUnixTime(str: String) = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
    dateFormat.parse(str).getTime
  }

  /**
   * 获得当前时间
   * @return 返回当前时间
   */
  def getNowDate = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    dateFormat.format(now)
  }

  /**
   * 获得当前日期
   * @return 返回当天日期
   */
  def getTimeFromUnix(d: Long): String = {
    val now: Date = new Date(d)
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    dateFormat.format(now)
  }

  /**
    * 获得指定格式当前日期
    * dtype格式
    * @return 返回当天日期
    */
  def getTimeFromUnix(d: Long,dtype:String): String = {
    val now: Date = new Date(d*1000)
    val dateFormat: SimpleDateFormat = new SimpleDateFormat(dtype)
    dateFormat.format(now)
  }

  //如传入2015-11-12 17:00:00，则返回2015-11-12 16:55:01
  /**
   * 获得给定时间之前最近的整5分钟的时间
   * @param str 给定时间
   * @return 返回给定时间之前最近的整5分钟的时间
   */
  def getEndTime(str: String) = {
    val reg(d, min) = str
    val ab = new ArrayBuffer[Any]()
    if (min < "05") {
      ab +=(d, "00", "00")
    } else {
      val r = min.toInt % 5
      val res = min.toInt - r
      if (res < 10) {
        val result = "0" + res.toString
        ab +=(d, result, "00")
      } else
        ab +=(d, res, "00")
    }
    ab.mkString(":")
  }

  /**
   * 获得给定时间之后最近的整5分钟的时间
   * @param str 给定时间
   * @return 返回给定时间之前最近的整5分钟的时间
   */
  def getNext5Min(str: String) = {
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val cal: Calendar = Calendar.getInstance()

    val reg(d, min) = str
    val ab = new ArrayBuffer[Any]()
    val r = min.toInt / 5
    val minStr = ((r + 1) * 5).toString
    ab +=(d, minStr, "00")
    val timeStr = ab.mkString(":")
    val t = df.parse(timeStr)
    cal.setTime(t)
    df.format(cal.getTime)
  }



  /**
   * 获得给定时间的第n天前的时间
   * @param str 给定时间
   * @param n 指定时间跨度
   * @return 返回给定时间的第n天前的时间
   */
  def getStartTime(str: String, n: Int) = {
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val cal: Calendar = Calendar.getInstance()
    val d = df.parse(str)
    cal.setTime(d)
    cal.add(Calendar.DATE, n)
    df.format(cal.getTime)
  }

  /**
    * 获得给定时间的第n小时前的时间
    *
    * @param dateStr 给定时间 format(yyyyMMddHHmmss)
    * @param n       指定时间跨度
    * @return 返回给定时间的第n小时的时间
    */
  def getNhour(dateStr: String, n: Int = -1) = {
    val df: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    val cal: Calendar = Calendar.getInstance()
    val dt = df.parse(dateStr)
    cal.setTime(dt)
    cal.add(Calendar.HOUR, n)
    df.format(cal.getTime).slice(0, 10) + "0000"
  }

  /**
    * 获得给定时间的第n天前的时间
    *
    * @param dateStr 给定时间 format(yyyyMMddHHmmss)
    * @param n       指定时间跨度
    * @return 返回给定时间的第n天的时间
    */
  def getNday(dateStr: String, n: Int = -1) = {
    val df: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    val cal: Calendar = Calendar.getInstance()
    val dt = df.parse(dateStr)
    cal.setTime(dt)
    cal.add(Calendar.DATE, n)
    df.format(cal.getTime).slice(0, 8) + "000000"
  }

  /**
    * 获取前后M个月的日期
    * @param month 日期 YYYY-MM
    * @param btype  日期格式 YYYY-MM
    * @param number 前后月份 -1,1
    * @return YYYY-MM
    */
  def getMonth(month:String,btype:String,number:Int)={
    val df: SimpleDateFormat = new SimpleDateFormat(btype)
    val date=df.parse(month)
    println(date)
    val cal:Calendar = Calendar.getInstance();
    cal.setTime(date)
    cal.add(Calendar.MONTH,-1)
    df.format(cal.getTime)
  }




  def main(args: Array[String]): Unit = {
//    val a = getStartTime("2015-01-08", -3)
//    println(a)
    //println(a < "2015-01-07")
    //val str = "1455939181519,115,1,19,42,4,0,-73,-3,30,1650,460,460,HTC M8w,1.0,351694067537394,0,"
    //println(str.replaceAll("(\\d{13})", "$1"))
//    println(getNext5Min("2016-03-17 16:57:09"))
//    println("1,5,7".contains("7"))

    println(getNext5Min("2016-03-17 16:57:09"))
    println("1,5,7".contains("7"))


    println(getNext5Min("2016-03-17 16:57:09"))
    println(getTimeFromUnix(1462171118L,"yyyyMM"))



  }
}
