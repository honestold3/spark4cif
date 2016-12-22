package com.finupgroup.cif.utils

import java.sql.{ResultSet, DriverManager}

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.{JdbcRDD, RDD}

/**
 * Created by s
 * on 2016-1-6
 */
object JDBCUtils {
  val prop = ConfigUtils.getConfig("/config/jdbc.properties")
  val log = Logger.getLogger(getClass)

  def main(args: Array[String]) {
    //    query()
    //    update()
    //    val start = System.currentTimeMillis()
    //    update2()
    //    println((System.currentTimeMillis() - start))
    val conn = getConnection("oracle")
    conn.close()
  }

  /**
   * 连接指定类型数据库
   * @param dbType 指定数据库类型
   * @return java.sql.connection
   */
  def getConnection(dbType: String) = {
    val keyFullname = ConfigUtils.getKeyFullname(dbType, "driver", "url", "username", "password")
    val Seq(driver, url, username, password) = ConfigUtils.getProps(prop, keyFullname: _*)
    Class.forName(driver)
    val conn = DriverManager.getConnection(url, username, password)
    log.info("--------------------------------------------------")
    log.info(s"\n$dbType already connected!\nurl is $url")
    log.info("--------------------------------------------------")
    conn
  }


  /**
   * 根据指定RDD的sql语句更新数据库
   * @param dbType 数据库类型
   * @param rdd 需要更新的sql语句 rdd
   */
  def update(dbType: String, rdd: RDD[String]) {
    rdd.foreachPartition { iter =>
      val conn = getConnection(dbType)
      try {
        conn.setAutoCommit(false)
        val prpe = conn.createStatement()
        iter.foreach { x =>
          prpe.addBatch(x)
        }
        prpe.executeBatch()
        conn.commit()
      } finally {
        conn.close()
      }
    }
  }
}
