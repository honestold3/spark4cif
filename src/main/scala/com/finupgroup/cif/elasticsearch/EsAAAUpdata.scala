package com.finupgroup.cif.elasticsearch

import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import com.puhui.aes.AesEncryptionUtil

/**
  * Created by wq on 2017/6/9.
  */
object EsAAAUpdata {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .set("es.nodes",args(0))
      .set("es.port",args(1))
      .setAppName("es_aaa_updata")

    val q = """{
              |   "query":{
              |        "bool": {
              |        	"must_not": {
              |    					"match": {
              |							"idCardNum": {
              |								"query": "AAA",
              |								"type": "phrase"
              |							}
              |						}
              |					}
              |		    }
              |    }
              |}""".stripMargin

    val sc = new SparkContext(conf)

    val rdd = sc.esRDD(s"testdata/AAA",q)

    val newrdd = rdd.mapPartitions {iter =>
      iter.map { x =>
        val key = x._1.toString
        val data = x._2

        val esdata = data.map{
          case x if x._1 == "idCardNum" => x._1 -> AesEncryptionUtil.encrypt(x._2.toString)
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

    newrdd.saveToEs(s"cif/indexFeatureToHbaseRowkey",Map("es.mapping.id"->"id"))
    //newrdd.collect().foreach(println)

    //newrdd.saveAsObjectFile("/es_aaa")
  }

}
