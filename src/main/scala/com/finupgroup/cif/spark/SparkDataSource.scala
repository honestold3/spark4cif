package com.finupgroup.cif.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import java.util.Properties
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.sql.types.{DataTypes,StringType, IntegerType, StructField, StructType}

/**
  * Created by wq on 2017/2/24.
  */
object SparkDataSource {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("SparkMysqlDemo")
      .setMaster("spark://honest:7077")
      .set("spark.executor.memory","1g")

    val sc = new SparkContext(sparkConf)

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._


    val jdbcDF = sqlContext.read.format("jdbc").options(
      Map(
        "url" -> "jdbc:mysql://honest:3306/test",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "tags",
        "user" -> "root",
        "password" -> ""
      )
    ).load()
    jdbcDF.show()
    println(jdbcDF.count())
    println(jdbcDF.rdd.partitions.size)


    println("-----------------------")

    val tagsRDD = sc.parallelize(Array("17 7 db1", "18 7 db2", "19 7 db3")).map(_.split(" "))
    val schema = StructType(
      List(
        StructField("tid", DataTypes.IntegerType),
        StructField("bookid", DataTypes.IntegerType),
        StructField("content", DataTypes.StringType)
      )
    )

    val rowRDD = tagsRDD.map(p => Row(p(0).toInt, p(1).toInt, p(2).trim))

    val mysqldf = sqlContext.createDataFrame(rowRDD, schema)

    val prop = new Properties()
    prop.put("driver","com.mysql.jdbc.Driver")
    prop.put("user", "root")
    prop.put("password", "")

    mysqldf.write.mode("append").jdbc("jdbc:mysql://honest:3306/test", "tags", prop)

  }

}
