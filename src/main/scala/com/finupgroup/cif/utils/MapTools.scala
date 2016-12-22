package com.finupgroup.cif.utils

import java.sql.ResultSet

import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD

/**
 * Created by zcq 2016-6-21.
 */
object MapTools {
  val globalProp = ConfigUtils.getConfig("/config/global.properties")
  val dicTabInfo = ConfigUtils.getConfig("/config/dictionaryTab.properties")
  val carrierConfig = ConfigUtils.getConfig("/config/carrierConfig.properties")

  def getTabMap(sc: SparkContext, id: String) = {
    sc.setLogLevel("WARN")
    val Seq(sql, key, value) = ConfigUtils.getProps(dicTabInfo, s"$id.sql", s"$id.key", s"$id.value")
    println(s"sql--->$sql")
    def flatValue(result: ResultSet) = {

      val keyidRes = key.split(",").map { x => result.getString(x.trim) }.mkString(",")
      val valueidRes = value.split(",").map { x => result.getString(x.trim) }.mkString(",")

      (keyidRes, valueidRes)
    }
    new JdbcRDD(
      sc,
      () => JDBCUtils.getConnection(globalProp.getOrElse("DATA_BASE", "")),
      sql,
      0,
      Long.MaxValue,
      1,
      flatValue
    ).collectAsMap()

  }

  def getTabRdd(sc: SparkContext, id: String) = {
    val Seq(sql, key, value) = ConfigUtils.getProps(dicTabInfo, s"$id.sql", s"$id.key", s"$id.value")
    println(s"sql--->$sql")
    def flatValue(result: ResultSet) = {
      val keyidRes = key.split(",").map { x => result.getString(x.trim) }.mkString(",")
      val valueidRes = value.split(",").map { x =>  result.getString(x.trim) }.mkString(",")
      (keyidRes, valueidRes)
    }
    new JdbcRDD(
      sc,
      () => JDBCUtils.getConnection(globalProp.getOrElse("DATA_BASE", "")),
      sql,
      0,
      Long.MaxValue,
      1,
      flatValue
    )

  }

  /**
    * 返回运营商名称
    * key  mcc_mnc
    * values carrier_id,carrier_Name
    */
  def getcarrierName(): scala.collection.Map[String, String] = {
    carrierConfig
  }

  def getPointConf(sc: SparkContext, id: String) = {
    val Seq(sql, key) = ConfigUtils.getProps(dicTabInfo, s"$id.sql", s"$id.key")
    println(s"sql--->$sql")
    def flatValue(result: ResultSet) = {
      key.split(",").map { x =>
        val str = result.getString(x.trim)
        if(str.startsWith(".")){
          "0"+str
        }else{
          str
        }

      }.mkString(",")
    }
    new JdbcRDD(
      sc,
      () => JDBCUtils.getConnection(globalProp.getOrElse("DATA_BASE", "")),
      sql,
      0,
      Long.MaxValue,
      1,
      flatValue
    )
  }


  def main(args: Array[String]) {
    val sc = new SparkContext("local","maptools")
    val m = getTabMap(sc,"appEvaluate_mdmDataDict")

    m.foreach(println(_))
    println(getcarrierName().getOrElse("460,0",""))
  }
}