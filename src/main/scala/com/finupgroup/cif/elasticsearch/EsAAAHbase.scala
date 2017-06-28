package com.finupgroup.cif.elasticsearch

import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import com.puhui.aes.AesEncryptionUtil
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get}
import org.apache.hadoop.hbase.util.Bytes

/**
  * Created by wq on 2017/6/6.
  */
object EsAAAHbase {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("es_aaa_hbase")
    val sc = new SparkContext(conf)

    val zk = args(0)
    val zkport = args(1)
    val tablename = args(2)



    val rdd = sc.textFile("/hbaseData")

    val newrdd = rdd.mapPartitions{ iter =>
      val hbaseconf = HBaseConfiguration.create()
      hbaseconf.set("hbase.zookeeper.property.clientPort", zkport)
      hbaseconf.set("hbase.zookeeper.quorum", zk)
      val conn = ConnectionFactory.createConnection(hbaseconf)
      val userTable = TableName.valueOf(tablename)
      val table = conn.getTable(userTable)
      import scala.collection.JavaConverters._

      iter.map{ x=>
        val values = x.split(",")
        val applyNo = values(0)
        val idCardNum = values(1)
        val rowkey = values(2)
        val rowseg = "@@@@"
        val cluseg = "####"

        val g = new Get(rowkey.getBytes)
        val result = table.get(g)
        if(result.isEmpty){
          s"rowkey:$rowkey,isNot"
        } else {
          var datalist = new scala.collection.mutable.ListBuffer[String]
          val list = result.listCells().asScala
          //for(cell: Cell <-list){
          list.foreach{ cell =>
            val rowKey = Bytes.toString(CellUtil.cloneRow(cell))
            val timestamp = cell.getTimestamp()
            val family = Bytes.toString(CellUtil.cloneFamily(cell))
            val qualifier = Bytes.toString(CellUtil.cloneQualifier(cell))
            val value = Bytes.toString(CellUtil.cloneValue(cell))
            println(s"$rowKey$cluseg$timestamp$cluseg$timestamp$cluseg$family$cluseg$qualifier$cluseg$value")
            datalist += s"$rowKey$cluseg$timestamp$cluseg$family$cluseg$qualifier$cluseg$value"
          }
          var datavalue = new java.lang.StringBuilder()
          println(datalist)
          datalist.toList.foreach{x=> datavalue.append(s"${x.toString}$rowseg")}
          datavalue.toString
        }

      }
    }

    newrdd.saveAsTextFile("/backuphbase")

  }

}
