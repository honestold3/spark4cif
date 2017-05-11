package com.finupgroup.cif.elasticsearch

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.elasticsearch.spark._
import com.puhui.aes.AesEncryptionUtil

/**
  * Created by wq on 2017/4/12.
  */
object EsRead {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .set("es.nodes", args(0))
      .set("es.port", args(1))
      .setAppName("es_read")

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
      "encrypt"->"0"
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

//    val map1 = map.map(x=>x._1->(x._2+"1"))
//    val list = List(map1)

    val list = List(map2)
    val input = sc.parallelize(list)

    input.saveToEs(s"cif/indexFeatureToHbaseRowkey")
  }

}
