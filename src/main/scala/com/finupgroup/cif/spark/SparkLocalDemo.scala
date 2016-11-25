package com.finupgroup.cif.spark


import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

/**
  * Created by wq on 2016/11/24.
  */
object SparkLocalDemo {

  def main(args: Array[String]): Unit = {


    val sc = new SparkContext("local","SparkDemo")

    val list = List("hello world","hi hello","hello world")

    sc.parallelize(list).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).collect().foreach(println)

  }

}
