package com.finupgroup.cif.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

/**
  * Created by wq on 2016/12/19.
  */
object WorldCount {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("Sparkdemo")
      .setMaster("spark://honest:7077")
      .set("spark.executor.memory","1g")

    val sc = new SparkContext(sparkConf)
    counts(sc,List("hello world"))

  }

  def counts(sc: SparkContext, list: List[String]) ={

      sc.parallelize(list).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).collect

  }

}
