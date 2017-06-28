package com.finupgroup.cif.elasticsearch

import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import com.puhui.aes.AesEncryptionUtil

/**
  * Created by wq on 2017/6/7.
  */
object EsAAACheck {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .set("es.nodes", args(0))
      .set("es.port", args(1))
      .setAppName("es_aaa_check")
    val sc = new SparkContext(conf)

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

    val rdd = sc.esRDD(s"testdata/AAA",q)

    val file  = rdd.map{x=>
      val key = x._1.toString
      val data = x._2
      val applyNo = data.getOrElse("applyNo", "")
      val idCardNum = data.getOrElse("idCardNum","")
      val rowkey = data.getOrElse("rowkey","")
      s"$applyNo,$idCardNum,$rowkey"
    }

    file.saveAsTextFile("/hbaseData")

  }
}
