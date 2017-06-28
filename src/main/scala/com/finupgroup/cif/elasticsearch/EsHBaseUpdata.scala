package com.finupgroup.cif.elasticsearch

import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import com.puhui.aes.AesEncryptionUtil
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Put}
import org.apache.hadoop.hbase.util.Bytes

/**
  * Created by wq on 2017/6/8.
  */
object EsHBaseUpdata {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("es_hbase_updata")
    val sc = new SparkContext(conf)

    val zk = args(0)
    val zkport = args(1)
    val tablename = args(2)

    val rdd = sc.textFile("/hbaseData")

    val newrdd = rdd.foreachPartition{iter =>
      val hbaseconf = HBaseConfiguration.create()
      hbaseconf.set("hbase.zookeeper.property.clientPort", zkport)
      hbaseconf.set("hbase.zookeeper.quorum", zk)
      val conn = ConnectionFactory.createConnection(hbaseconf)
      val userTable = TableName.valueOf(tablename)
      val table = conn.getTable(userTable)
      import scala.collection.JavaConverters._

      iter.foreach{ x=>
        val values = x.split(",")
        val applyNo = values(0)
        val idCardNum = values(1)
        val rowkey = values(2)
        val g = new Get(rowkey.getBytes)
        val result = table.get(g)
        if(!result.isEmpty){
          val list = result.listCells().asScala
          list.foreach{ cell =>
            val rowKey = Bytes.toString(CellUtil.cloneRow(cell))
            val family = Bytes.toString(CellUtil.cloneFamily(cell))
            val qualifier = Bytes.toString(CellUtil.cloneQualifier(cell))
            val value = Bytes.toString(CellUtil.cloneValue(cell)).replaceAll(""""idNum":"AAA"""",s""""idNum":"$idCardNum"""")
            val p = new Put(rowKey.getBytes)
            p.addColumn(family.getBytes,qualifier.getBytes, value.getBytes)
            table.put(p)
          }
        }
      }
    }
  }
}
