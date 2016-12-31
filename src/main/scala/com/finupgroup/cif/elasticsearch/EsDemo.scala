package com.finupgroup.cif.elasticsearch

import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext


/**
  * Created by wq on 2016/12/23.
  */
object EsDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .set("es.nodes","honest")
      .set("es.port","9200")
      .setMaster("spark://honest:7077")
      .setAppName("es_spark")

    val sc = new SparkContext(conf)
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    //查询为abc的数据
    val query = """{"query":{"match":{"activity.partnerCode": "abc"}}}"""

    //将在es中的查询结果转化为rdd/dataFrame
    val esRdd = sc.esRDD(s"index/type",query)
    //esRdd.map(xx _ ).saveToEs("index/ss")
    //直接读入全部数据
    val esDf = sqlContext.esDF(s"index/type")

  }

}
