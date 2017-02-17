package com.finupgroup.cif.elasticsearch

import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import com.puhui.aes.AesEncryptionUtil


/**
  * Created by wq on 2016/12/23.
  */
object EsDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .set("es.nodes","honest")
//      .set("es.port","9200")
      //.setMaster("spark://honest:7077")
      .set("es.nodes",args(0))
      .set("es.port",args(1))
//      .setMaster(args(2))
      .setAppName("es_spark")

    val sc = new SparkContext(conf)
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    //查询为abc的数据
    //val query = """{"query":{"match":{"activity.partnerCode": "abc"}}}"""

    //将在es中的查询结果转化为rdd/dataFrame
    //val esRdd = sc.esRDD(s"index/type",query)
    //esRdd.map(xx _ ).saveToEs("index/ss")
    //直接读入全部数据
    //val esDf = sqlContext.esDF(s"index/type")

    //val esrdd = sc.esRDD(s"flink-test2/fink_test_data2")
    //esrdd.collect.foreach(println)
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
      "encrypt"->"1"
    )

    val map2 = Map(
      "cfType"->"cfType",
      "channel"->"channel",
      "insertDate"->"insertDate",
      "className"->"className",
      "storeid"->"storeid",
      "taobaoAccount"->"taobaoAccount",
      "biz"->"biz1"
       //"id"->"kdieimkkkk"
    )

    val map1 = map.map(x=>x._1->(x._2+"1"))

    //val list = List(map,map1)
    //val list = List(map2)
    //val input = sc.parallelize(list)
    //input.saveToEs(s"cif/indexFeatureToHbaseRowkey",Map("es.mapping.id"->"id"))
    //input.saveToEs(s"cif/indexFeatureToHbaseRowkey")

    val mdata = List("idCardNum","mobilePhone","emal","cardNum","creditCardNumber")
    val q = """{"query":{"match":{"encrypt": "1"}}}"""
    val q1 = """{"query":{"match":{"biz": "biz1"}}}"""
    val q2 = """{"query":{"bool":{"must_not" : {"term" : {"encrypt" : "1"}}}}}"""
    val q3 = """{"query":{"bool":{"must_not" : {"term" : {"oencrypt" : "nothing"}}}}}"""
    val rdd = sc.esRDD(s"cif/indexFeatureToHbaseRowkey",q2)

//    println("---------")
//    println(rdd.collect().length)
//    rdd.collect().foreach(println)
//    println("----------")

    val newrdd = rdd.map{x=>
      val data = x._2
      data.map{
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
        case x if x._1 == "id" => x._1 -> x._2.toString
        case x if x._1 == "_id" => "id" -> x._2.toString
        //case _ => x._1->x._2.toString()
      }

    }

    //newrdd.collect.foreach(println)
    //newrdd.saveToEs(s"cif/indexFeatureToHbaseRowkey")

    newrdd.saveAsObjectFile("/es")
//    sc.objectFile[scala.collection.immutable.HashMap[String,String]]("/es").foreach{x=>
//      println(x)
//      println(x.getOrElse("biz","wwwww"))
//      println(x.isInstanceOf[Map[String,String]])
//      println("###############")
//    }
//
    //sc.objectFile[scala.collection.immutable.HashMap[String,String]]("/es").saveToEs(s"cif/indexFeatureToHbaseRowkey",Map("es.mapping.id"->"id"))

  }

}
