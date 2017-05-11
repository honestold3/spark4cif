package com.finupgroup.cif.elasticsearch

import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import com.puhui.aes.AesEncryptionUtil

import org.json4s.JsonDSL.WithDouble._
import org.json4s._
import org.json4s.jackson.JsonMethods._


/**
  * Created by wq on 2017/4/12.
  */
object EsData {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //      .set("es.nodes","honest")
      //      .set("es.port","9200")
      //.setMaster("spark://honest:7077")
      .set("es.nodes", args(0))
      .set("es.port", args(1))
      //.set("es.mapping.id", "_id")
      //      .setMaster(args(2))
      .setAppName("es_data")

    val sc = new SparkContext(conf)

    val q = """{
              |     "query" : {
              |        "filtered" : {
              |            "filter": {
              |                "missing" : { "field" : "columnType" }
              |            }
              |        }
              |    }
              |}""".stripMargin

    val rdd = sc.esRDD(s"cif/indexFeatureToHbaseRowkey",q)

    val ids = rdd.map(x => x._1)

    ids.saveAsTextFile("/es_ids2")

  /*  val wholeRdd = sc.esRDD(s"cif/indexFeatureToHbaseRowkey")


    val newrdd = wholeRdd.map{ x=>
      val key = x._1
      val data = x._2.asInstanceOf[scala.collection.mutable.Map[String,String]]

      val esdata = data.map{
        case x if x._1 == "idCardNum" => x._1 -> x._2.toString
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
      esdata + ("id"-> key)
      val json = compact(render(esdata))
      json
    }
    //newrdd.collect().foreach(println)
    newrdd.saveAsTextFile("/es24_whole")
    */
  }

}
