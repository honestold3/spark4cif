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
    //counts(sc,List("hello world"))

    val rdd2 = sc.parallelize(List((1,"A"),(2,"B")))
    val rdd3 = sc.parallelize(List((1,"AA"),(2,"BB")))
    val bc = sc.broadcast(rdd3.collectAsMap)

    kankanBC(sc,rdd2,bc.value.asInstanceOf[scala.collection.mutable.HashMap[Int,String]])

  }

  def counts(sc: SparkContext, list: List[String]) ={

      sc.parallelize(list).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).collect

  }

  def kankanBC(sc: SparkContext, rdd: RDD[(Int,String)],map: scala.collection.mutable.HashMap[Int,String]): Unit ={

    rdd.mapPartitions{iter =>
      //val m = bc.value
      val m = map
      iter.map{
        case (k,v) => (k,(v,m.get(k).get))
      }
    }.collect.foreach(println)

  }

}
