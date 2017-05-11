package com.finupgroup.cif.elasticsearch

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.elasticsearch.spark._
import com.puhui.aes.AesEncryptionUtil

/**
  * Created by wq on 2017/2/21.
  */
object EsNew {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .set("es.nodes",args(0))
      .set("es.port",args(1))
      .setAppName("es_upload")

    val sc = new SparkContext(conf)

//    sc.objectFile[scala.collection.immutable.HashMap[String,String]]("/es").foreach{x=>
//      println(x)
//      println(x.getOrElse("biz","wwwww"))
//      println(x.isInstanceOf[Map[String,String]])
//      println("###############")
//    }

    sc.objectFile[scala.collection.immutable.HashMap[String,String]]("/es_tmp")
      .saveToEs(s"cif/indexFeatureToHbaseRowkey",Map("es.mapping.id"->"id"))
  }

}
