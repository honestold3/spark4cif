package com.finupgroup.cif.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/**
  * Created by wq on 2016/12/9.
  */
object ParquetDemo {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("Sparkdemo")
      .setMaster("spark://honest:7077")
      .set("spark.executor.memory","7g")

    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    //val sqlContext = SQLContext.getOrCreate(sc)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    case class Person(id: Int, age: Int)

    val rdd = sc.parallelize(Array(Row(1, 30), Row(2, 29), Row(4, 21)))
    val schema = StructType(Array(StructField("id", DataTypes.IntegerType), StructField("age", DataTypes.IntegerType)))
    val df = sqlContext.createDataFrame(rdd, schema)
    df.write.mode("append").parquet("/parquet1")

  }

}
