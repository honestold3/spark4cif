package com.finupgroup.cif.spark

//import com.alexholmes.hadooputils.combine.avro.mapred.CombineAvroInputFormat
import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import com.databricks.spark.avro._
import com.finupgroug.cif.combine.CombineAvroKeyInputFormat
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroKeyInputFormat
import org.apache.spark.deploy.SparkHadoopUtil

/**
  * Created by wq on 2017/5/5.
  */
object AvroDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("spark_avro")

    val sc = new SparkContext(conf)
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    sc.hadoopConfiguration.set("mapred.max.split.size", (50*1024*1024).toString)
    sc.hadoopConfiguration.set("mapred.min.split.size", (50*1024*1024).toString)

    val df = Seq(
      (2012, 8, "Batman", 9.8),
      (2012, 8, "Hero", 8.7),
      (2012, 7, "Robot", 5.5),
      (2011, 7, "Git", 2.0)).toDF("year", "month", "title", "rating")

    //df.write.mode(SaveMode.Append).partitionBy("year", "month").avro("/avro")

//    val file = sc.newAPIHadoopFile[AvroKey[GenericRecord], NullWritable, CombineAvroKeyInputFormat[GenericRecord]]("/avro")
//    println("ddddddddddd::::"+file.count())
//    file.collect().foreach(println)
//    val kankan = file.toDF("year", "month", "title", "rating")
//    kankan.show()
    //val aaaa = sc.newAPIHadoopFile("avro",classOf[CombineAvroInputFormat],classOf[AvroKey[GenericRecord]],classOf[NullWritable])
    val avro = sqlContext.read.avro("/avro")
    println("count::::"+avro.count())
    avro.show()

  }

}
