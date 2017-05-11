package com.finupgroup.cif.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat
import org.apache.spark.sql._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/**
  * Created by wq on 2016/12/2.
  */
object ParquestDemo {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("Sparkdemo")
      .setMaster("spark://honest:7077")
      .set("spark.executor.memory","7g")

    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    case class Person(id: Int, age: Int)

    val idAgeRDDPerson = sc.parallelize(Array(Person(1, 30), Person(2, 29), Person(3, 21)))

    val sqlContext = new SQLContext(sc)
    /**
      * id      age
      * 1       30
      * 2       29
      * 3       21
      */
    val idAgeRDDRow = sc.parallelize(Array(Row(1, 30), Row(2, 29), Row(4, 21)))

    val schema = StructType(Array(StructField("id", DataTypes.IntegerType), StructField("age", DataTypes.IntegerType)))

    val idAgeDF = sqlContext.createDataFrame(idAgeRDDRow, schema)

    //idAgeDF.write.mode("append").parquet("/parquest")
//    val rdd = sc.textFile("/wq/wc.txt")
//    rdd.flatMap{x => val ss = x.split(" ");ss.map((_,1))}.reduceByKey(_+_).collect.foreach(println)

    //val sqlContext = SQLContext.getOrCreate(sc)

    val idAgeRDDRow1 = sc.parallelize(List(Row(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29, 30)))

    val schema1 = StructType(Array(StructField("c1", DataTypes.IntegerType),
      StructField("c2", DataTypes.IntegerType),
      StructField("c3", DataTypes.IntegerType),
      StructField("c4", DataTypes.IntegerType),
      StructField("c5", DataTypes.IntegerType),
      StructField("c6", DataTypes.IntegerType),
      StructField("c7", DataTypes.IntegerType),
      StructField("c8", DataTypes.IntegerType),
      StructField("c9", DataTypes.IntegerType),
      StructField("c10", DataTypes.IntegerType),
      StructField("c11", DataTypes.IntegerType),
      StructField("c12", DataTypes.IntegerType),
      StructField("c13", DataTypes.IntegerType),
      StructField("c14", DataTypes.IntegerType),
      StructField("c15", DataTypes.IntegerType),
      StructField("c16", DataTypes.IntegerType),
      StructField("c17", DataTypes.IntegerType),
      StructField("c18", DataTypes.IntegerType),
      StructField("c19", DataTypes.IntegerType),
      StructField("c20", DataTypes.IntegerType),
      StructField("c21", DataTypes.IntegerType),
      StructField("c22", DataTypes.IntegerType),
      StructField("c23", DataTypes.IntegerType),
      StructField("c24", DataTypes.IntegerType),
      StructField("c25", DataTypes.IntegerType),
      StructField("c26", DataTypes.IntegerType),
      StructField("c27", DataTypes.IntegerType),
      StructField("c28", DataTypes.IntegerType),
      StructField("c29", DataTypes.IntegerType),
      StructField("c30", DataTypes.IntegerType)
    ))

    val df = sqlContext.createDataFrame(idAgeRDDRow1,schema1)

    df.write.mode(SaveMode.Append)parquet("hdfs://honest:8020/p3")

    sqlContext.read.parquet("hdfs://honest:8020/p3").collect().foreach(println)

    sc.hadoopConfiguration.set("mapred.max.split.size", (50*1024*1024).toString)
    sc.hadoopConfiguration.set("mapred.min.split.size", (50*1024*1024).toString)

    import sqlContext.implicits._
    val file = sc.newAPIHadoopFile[LongWritable, Text, CombineTextInputFormat] ("hdfs://honest:8020/small")

    val text = sc.textFile("hdfs://honest:8020/test1")

    val rowRDD = file.map(_._2.toString.split(" ")).filter(_.length == 26).map{e =>
      Row(e(0), e(1), e(2), e(3), e(4), e(5), e(6), e(7), e(8), e(9), e(10),
               e(11), e(12),e(13),e(14),e(15), e(16), e(17),e(18),e(19),
              e(20), e(21), e(22), e(23), e(24), e(25)
      )
    }
//    val rowRDD = file.map{x =>
//      val e = x._2.toString.split(" ")
//      Row(e(0), e(1), e(2), e(3), e(4), e(5), e(6), e(7), e(8), e(9), e(10),
//        e(10), e(11), e(12), e(13), e(14), e(15), e(16), e(17), e(18), e(19),
//        e(20), e(21), e(22), e(23), e(24), e(25)
//      )
//    }

//    val rowRDD = file.map{ x=>
//      val rows = x.toString.split(" ")
//      rows.filter(x => x.length==26).map(e =>
//        Row(e(0), e(1), e(2), e(3), e(4), e(5), e(6), e(7), e(8), e(9), e(10),
//          e(10), e(11), e(12), e(13), e(14), e(15), e(16), e(17), e(18), e(19),
//          e(20), e(21), e(22), e(23), e(24), e(25)
//        )
//      )
//    }

    //rowRDD.saveAsTextFile("hdfs://honest:8020/small222")

    val schema2 = StructType(Array(
      StructField("c1", DataTypes.StringType),
      StructField("c2", DataTypes.StringType),
      StructField("c3", DataTypes.StringType),
      StructField("c4", DataTypes.StringType),
      StructField("c5", DataTypes.StringType),
      StructField("c6", DataTypes.StringType),
      StructField("c7", DataTypes.StringType),
      StructField("c8", DataTypes.StringType),
      StructField("c9", DataTypes.StringType),
      StructField("c10", DataTypes.StringType),
      StructField("c11", DataTypes.StringType),
      StructField("c12", DataTypes.StringType),
      StructField("c13", DataTypes.StringType),
      StructField("c14", DataTypes.StringType),
      StructField("c15", DataTypes.StringType),
      StructField("c16", DataTypes.StringType),
      StructField("c17", DataTypes.StringType),
      StructField("c18", DataTypes.StringType),
      StructField("c19", DataTypes.StringType),
      StructField("c20", DataTypes.StringType),
      StructField("c21", DataTypes.StringType),
      StructField("c22", DataTypes.StringType),
      StructField("c23", DataTypes.StringType),
      StructField("c24", DataTypes.StringType),
      StructField("c25", DataTypes.StringType),
      StructField("c26", DataTypes.StringType)
    ))



    val df2  = sqlContext.createDataFrame(rowRDD,schema2)

    df2.write.mode(SaveMode.Append).parquet("hdfs://honest:8020/p66")

    val kankan = sqlContext.read.parquet("hdfs://honest:8020/p66").count()

    println(s"kankan:$kankan")

  }

}
