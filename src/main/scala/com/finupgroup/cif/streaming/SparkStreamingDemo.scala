package com.finupgroup.cif.streaming

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

/**
  * Created by wq on 2016/11/24.
  */
object SparkStreamingDemo {

  def main(args: Array[String]): Unit = {

  }

  def restr(s: String) = {
    val len = s.length
    val ac = s.toCharArray
    val out = for {
      i <- Range(len,0,-1)
      //node = ac(i-1)
    } yield ac(i-1)
    out.mkString
  }

}
