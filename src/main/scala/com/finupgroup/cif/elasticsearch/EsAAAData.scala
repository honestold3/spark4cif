package com.finupgroup.cif.elasticsearch

import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import com.puhui.aes.AesEncryptionUtil

/**
  * Created by wq on 2017/6/5.
  */
object EsAAAData {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .set("es.nodes",args(0))
      .set("es.port",args(1))
      .setAppName("es_aaa")

    val q = """{"query":{"bool":{"must" : {"term" : {"idCardNum" : "xy833723939f720bcf023d3ab2f2c7f8d720160926"}}}}}"""
    val q2 = """{"query":{"bool":{"must" : {"term" : {"idCardNum" : "idCardNum"}}}}}"""

    val sc = new SparkContext(conf)

    val data = sc.textFile("/redata")
    val dataMap  = data.map{x=> val y = x.split(","); (y(0),y(1))}.collectAsMap()
    val bc = sc.broadcast(dataMap)

    val rdd = sc.esRDD(s"cif/indexFeatureToHbaseRowkey",q)

    val newrdd = rdd.mapPartitions {iter =>
      val m = bc.value
      iter.map { x =>
        val key = x._1.toString
        val data = x._2
        val applyNo = data.getOrElse("applyNo", "isNull")

        val esdata = data.map{
          case x if x._1 == "idCardNum" => x._1 -> m.getOrElse(applyNo.toString,"AAA")
          case x if x._1 == "mobilePhone" => x._1 -> x._2.toString
          case x if x._1 == "emal" => x._1 -> x._2.toString
          case x if x._1 == "cardNum" => x._1 -> x._2.toString
          case x if x._1 == "creditCardNumber" => x._1 -> x._2.toString
          case x if x._1 == "cfType" => x._1 -> x._2.toString
          case x if x._1 == "channel" => x._1 -> x._2.toString
          case x if x._1 == "insertDate" => x._1 -> x._2.toString
          case x if x._1 == "className" => x._1 -> x._2.toString
          case x if x._1 == "storeid" => x._1 -> x._2.toString
          case x if x._1 == "taobaoAccount" => x._1 -> x._2.toString
          case x if x._1 == "biz" => x._1 -> x._2.toString
          case x if x._1 == "rowkey" => x._1 -> x._2.toString
          case x if x._1 == "isUpdateThirdBlack" => x._1 -> x._2.toString
          case x if x._1 == "batchNo" => x._1 -> x._2.toString
          case x if x._1 == "originalApplyNo" => x._1 -> x._2.toString
          case x if x._1 == "columnType" => x._1 -> x._2.toString
          case x if x._1 == "ct" => x._1 -> x._2.toString
          case x if x._1 == "cusQQInfo" => x._1 -> x._2.toString
          case x if x._1 == "mobilePhone" => x._1 -> x._2.toString
          case x if x._1 == "oplogReceiveDate" => x._1 -> x._2.toString
          case x if x._1 == "createTime" => x._1 -> x._2.toString
          case x if x._1 == "applyNo" => x._1 -> x._2.toString
          case x if x._1 == "updateTime" => x._1 -> x._2.toString
          case x if x._1 == "idCardNum" => x._1 -> x._2.toString
          case x if x._1 == "id5" => x._1 -> x._2.toString
          case x if x._1 == "emal" => x._1 -> x._2.toString
          case x if x._1 == "cid" => x._1 -> x._2.toString
          case x if x._1 == "encrypt" => x._1 -> "0"
          case x if x._1 == "id" => "id" -> key
          case x if x._1 == "_id" => "id" -> x._2.toString
          case _ => x._1->x._2.toString()
        }
        esdata + ("id"->key)

      }
    }

    newrdd.saveToEs(s"testdata/AAA",Map("es.mapping.id"->"id"))
    //newrdd.collect().foreach(println)

    //newrdd.saveAsObjectFile("/es_aaa")
  }

}
