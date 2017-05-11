package com.finupgroup.cif.elasticsearch

import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import com.puhui.aes.AesEncryptionUtil

/**
  * Created by wq on 2017/3/21.
  */
object EsCheck {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .set("es.nodes",args(0))
      .set("es.port",args(1))
      .setAppName("es_check")

    val sc = new SparkContext(conf)

    val map = Map(
      "cfType"->"cfType",
      "channel"->"channel",
      "insertDate"->"insertDate",
      "className"->"className",
      "storeid"->"storeid",
      "taobaoAccount"->"taobaoAccount",
      "biz"->"biz",
      "cardNum"->"cardNum",
      "rowkey"->"rowkey",
      "isUpdateThirdBlack"->"isUpdateThirdBlack",
      "batchNo"->"batchNo",
      "originalApplyNo"->"originalApplyNo",
      "updateTime"->"updateTime",
      "columnType"->"columnType",
      "ct"->"ct",
      "cusQQInfo"->"cusQQInfo",
      "mobilePhone"->"mobilePhone",
      "oplogReceiveDate"->"oplogReceiveDate",
      "createTime"->"createTime",
      "creditCardNumber"->"creditCardNumber",
      "applyNo"->"applyNo",
      "idCardNum"->"idCardNum",
      "id5"->"id5",
      "emal"->"emal",
      "cid"->"cid",
      "encrypt"->"0",
      "id"->"123456789"
    )

    val list = List(map)
    val input = sc.parallelize(list)
    input.saveToEs(s"cif/indexFeatureToHbaseRowkey",Map("es.mapping.id"->"id"))
    //
    val rdd = sc.esRDD(s"cif/indexFeatureToHbaseRowkey")
    val newrdd = rdd.map{x=>
      val key = x._1.toString
      val data = x._2
      val esdata = data.map{
        case x if x._1 == "idCardNum" => x._1 -> AesEncryptionUtil.encrypt(x._2.toString)
        case x if x._1 == "mobilePhone" => x._1 -> AesEncryptionUtil.encrypt(x._2.toString)
        case x if x._1 == "emal" => x._1 -> AesEncryptionUtil.encrypt(x._2.toString)
        case x if x._1 == "cardNum" => x._1 -> AesEncryptionUtil.encrypt(x._2.toString)
        case x if x._1 == "creditCardNumber" => x._1 -> AesEncryptionUtil.encrypt(x._2.toString)
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

    //rdd.collect().foreach(println)

    //rdd.saveAsObjectFile("/es_data")

    //val hdfsrdd = sc.objectFile[(String,Map[String,String])]("/es_data")
    //hdfsrdd.collect.foreach(println)

    val acc = sc.accumulator(0,"kkkkk of error")
    val hdfsrdd =sc.objectFile[scala.collection.immutable.HashMap[String,String]]("/es")
    //hdfsrdd.collect().foreach(println)
    hdfsrdd.foreach{x => acc+=1;
      println(x.get("id"))
    }

    println("acc::::"+acc.value)
    //hdfsrdd.saveToEs("cif/indexFeatureToHbaseRowkey",Map("es.mapping.id"->"id"))




  }

}
