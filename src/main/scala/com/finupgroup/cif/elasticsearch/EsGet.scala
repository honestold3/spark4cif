package com.finupgroup.cif.elasticsearch

import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import com.puhui.aes.AesEncryptionUtil

/**
  * Created by wq on 2017/6/10.
  */
object EsGet {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("accum")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("/kankan")

    val accum = sc.accumulator(0, "Example Accumulator")

    rdd.foreach{ x =>
      if(x.contains("hello")){
        accum += 1
      }
    }

    println(s"accum::::${accum.value}")




    val ac= sc.accumulator(0, "Error Accumulator")
    val data = sc.parallelize(1 to 10)
    //用accumulator统计偶数出现的次数，同时偶数返回0，奇数返回1
    val newData = data.map{x => {
      if(x%2 == 0){
        ac += 1
        0
      }else 1
    }}
    //使用action操作触发执行
    //newData.count
    //此时accum的值为5，是我们要的结果
    println("ac:::"+ac.value)

    //继续操作，查看刚才变动的数据,foreach也是action操作
    newData.foreach(println)
    //上个步骤没有进行累计器操作，可是累加器此时的结果已经是10了
    //这并不是我们想要的结果
    println("ac:::"+ac.value)


  }

}
