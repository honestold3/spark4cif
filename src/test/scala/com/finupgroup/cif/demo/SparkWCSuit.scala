package com.finupgroup.cif.demo

import com.finupgroup.cif.spark.WorldCount
import com.finupgroup.cif.tools.LocalSparkContext
import org.apache.spark.sql.{Row, SQLContext}
import org.scalatest.FunSuite


/**
  * Created by wq on 2016/11/27.
  */
class SparkWCSuit extends FunSuite
  with LocalSparkContext {

  //rdd wordCount
  test("test rdd wc") {
    sc.setLogLevel("ERROR")
    val rdd = sc.makeRDD(Seq("a", "b", "b"))
    val res = rdd.map((_, 1)).reduceByKey(_ + _).collect().sorted
    assert(res === Array(("a", 1), ("b", 2)))
  }

  //df wordCount
  test("test df wc") {
    val head +: tail = List(1,2,3)
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    val df = sc.makeRDD(Seq("a", "b", "b")).toDF("word")
    val res = df.groupBy("word").count().collect()
    assert(res === Array(Row("a",1),Row("b",2)))
  }

  test("test WorldCount"){
    val list = List("hello world","hi hello","hello world")
    val result = WorldCount.counts(sc,list)
    assert(result.length == 3)
  }
}
