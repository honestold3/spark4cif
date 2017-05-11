package com.finupgroup.cif.spark

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

/**
  * Created by wq on 2016/11/25.
  */
object SparkDemo {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("Sparkdemo")
      .setMaster("spark://honest:7077")
      .set("spark.executor.memory","1g")

    val sc = new SparkContext(sparkConf)

    val rdd = sc.textFile("/wq/wc.txt")

    rdd.flatMap{x => val ss = x.split(" ");ss.map((_,1))}.reduceByKey(_+_).collect.foreach(println)

  }

}
