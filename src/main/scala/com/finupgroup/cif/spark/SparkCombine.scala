package com.finupgroup.cif.spark

import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import com.databricks.spark.avro._
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroKeyInputFormat

//import scala.collection.JavaConversions._

/**
  * Created by wq on 2017/5/5.
  */
object SparkCombine {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("spark_combine")

    val sc = new SparkContext(conf)
    val sqlContext = SQLContext.getOrCreate(sc)

    sc.hadoopConfiguration.set("mapred.max.split.size", (50*1024*1024).toString)
    sc.hadoopConfiguration.set("mapred.min.split.size", (50*1024*1024).toString)

    import sqlContext.implicits._
    //sqlContext.read.avro("")
//    val file = sc.newAPIHadoopFile[AvroKey[GenericRecord], NullWritable, CombineAvroInputFormat[GenericRecord]](args(0))
//    //sc.newAPIHadoopRDD[AvroKey[GenericRecord], NullWritable, CombineAvroInputFormat[GenericRecord]]("")
//    val df = file.toDF("name","id_no","loan_type","loan_status","loan_month","loan_amount","loan_period",
//    "current_status","owe_amount","data_logo","etl_date")
//
//    df.write.avro("/avro")


    //sqlContext.read.avro(args(0)).repartition(5).write.avro("/avro")

    //val file = sc.newAPIHadoopFile[LongWritable, Text, CombineTextInputFormat](args(0))
    //file.saveAsTextFile("/avro")

    sqlContext.read.avro(args(0)).repartition(args(1).toInt).write.avro("/avro")

  }

}
