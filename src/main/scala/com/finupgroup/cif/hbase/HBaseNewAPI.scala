package com.finupgroup.cif.hbase

import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

import scala.collection.JavaConversions._

/**
  * Created by wq on 2016/10/7.
  */
object HBaseNewAPI {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("SparkHBase")
    val sc = new SparkContext(conf)
    //val sc = new SparkContext("local", "SparkHBase")
    val hbaseconf = HBaseConfiguration.create()
    hbaseconf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseconf.set("hbase.zookeeper.quorum", "honest")


    val conn = ConnectionFactory.createConnection(hbaseconf)
    val admin = conn.getAdmin
    //admin.createNamespace(NamespaceDescriptor.create("cif").build());

    val userTable = TableName.valueOf("cif:cif_lob_channel")


    val tableDescr = new HTableDescriptor(userTable)
    tableDescr.addFamily(new HColumnDescriptor("basic".getBytes))
//    println("Creating table")
//    if (admin.tableExists(userTable)) {
//      admin.disableTable(userTable)
//      admin.deleteTable(userTable)
//    }
//    admin.createTable(tableDescr)
    println("Done!")

    try{
      val table = conn.getTable(userTable)

      try{
        val p = new Put("bb2cfa70b6c6b92ede2d6f3bf1ac966d".getBytes)
        p.addColumn("basic".getBytes,"name11".getBytes, """"statementCycle":"2016-07-07 00:00:00","idNum":"AAA","email":"lilei663485053@qq.com"""".getBytes)
        table.put(p)

        val g = new Get("bb2cfa70b6c6b92ede2d6f3bf1ac966d".getBytes)
        val result = table.get(g)
        if(result==null) println("ddddddddd")
        val value = Bytes.toString(result.getValue("basic".getBytes,"name".getBytes))
        println("GET id001 :"+value)
        println("----------------------------")

        result.listCells().foreach{ cell=>
          val rowKey = Bytes.toString(CellUtil.cloneRow(cell))
          val timestamp = cell.getTimestamp()
          val family = Bytes.toString(CellUtil.cloneFamily(cell))
          val qualifier = Bytes.toString(CellUtil.cloneQualifier(cell))
          val value = Bytes.toString(CellUtil.cloneValue(cell))
          println(s"===>rowKey:$rowKey,timestamp:$timestamp,family:$family,qualifier:$qualifier,value:$value")
        }

        val s = new Scan()
        s.addColumn("basic".getBytes,"name".getBytes)
        val scanner = table.getScanner(s)

        try{
          for(r <- scanner){
            println("Found row: "+r)
            println("Found value: "+Bytes.toString(r.getValue("basic".getBytes,"name".getBytes)))
          }
        }finally {
          scanner.close()
        }

        val d = new Delete("id001".getBytes)
        d.addColumn("basic".getBytes,"name".getBytes)
        //table.delete(d)

      }finally {
        if(table != null) table.close()
      }

    }finally {
      conn.close()
    }
  }
}
