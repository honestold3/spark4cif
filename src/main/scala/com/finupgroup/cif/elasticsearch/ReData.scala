package com.finupgroup.cif.elasticsearch

import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import com.puhui.aes.AesEncryptionUtil

/**
  * Created by wq on 2017/6/6.
  */
object ReData {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("ReData")
      .set("spark.broadcast.compress","true")

    val sc = new SparkContext(conf)

//    val rdd = sc.textFile("/user/hive/warehouse/dw.db/qianzhan_idno_1/part-*")
//
//    rdd.filter(x =>x.split(",")(1)!=" ").saveAsTextFile("/redata")
    //scala.collection.mutable.LinkedHashMap

    val esdata = sc.objectFile[scala.collection.mutable.LinkedHashMap[String,String]]("/es_aaa")

    val data = sc.textFile("/redata")
    val dataMap  = data.map{x=> val y = x.split(","); (y(0),y(1))}.collectAsMap()
    val bc = sc.broadcast(dataMap)

    val newrdd = esdata.mapPartitions{ iter =>
      val m = bc.value
      iter.map{ x =>
        val applyNo = x.getOrElse("applyNo","isNull")
        val idCardNum = x.getOrElse("idCardNum","AAA")
        if(applyNo!="isNull"){
          if(m.contains(applyNo)){
            x.updated("idCardNum",idCardNum)
          }
        } else {
          x
        }
      }
    }
//    val acc = sc.accumulator(0,"is no AAA")
//    newrdd.foreach{ x =>
//      val y = x.asInstanceOf[scala.collection.mutable.LinkedHashMap[String,String]]
//      if(y.get("idCardNum").get !="AAA"){
//        acc+=1
//      }
//    }
//
//    println("acc::::"+acc.value)

    newrdd.saveAsObjectFile("/es_noAAA")

  }

}
