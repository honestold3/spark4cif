package com.finupgroup.cif.utils

import java.util.Properties

import scala.collection.JavaConversions.propertiesAsScalaMap

/**
 * Created by wangxy
 * 2015-6-11.
 */

object ConfigUtils {
  /**
   * 读取配置文件
   * @param path 配置文件路径
   * @return 配置文件中配置的key,value对集合
   */
  def getConfig(path: String): scala.collection.Map[String, String] = {
    val prop = new Properties()
    val inputStream = this.getClass.getResourceAsStream(path)
    try {
      prop.load(inputStream)
      prop
    } finally inputStream.close()
  }

  /**
   * 根据传入参数返回其在properties配置文件中的value值
   * @param prop 配置文件对象
   * @param strs 配置文件中的key
   * @return value对应的数组
   */
  def getProps(prop: collection.Map[String, String], strs: String*) = {
    strs.map { x =>
      val value = prop.getOrElse(x, "")
      if ("" == value) throw new Exception(s"key:$x does not exist!")
      value
    }
  }

  /**
   * 根据传入参数返回其在properties配置文件中的value值--Int类型
   * @param prop 配置文件对象
   * @param strs 配置文件中的key
   * @return value对应的数组
   */
  def getProps2Int(prop: collection.Map[String, String], strs: String*) = {
    strs.map { x =>
      val value = prop.getOrElse(x, "")
      if ("" == value) throw new Exception(s"key:$x does not exist!")
      value.toInt
    }
  }

  /**
   * 返回拼接后的key
   * @param pre  key的前缀
   * @param strs 后缀
   * @return 完整的key
   */
  def getKeyFullname(pre: String, strs: String*) = {
    if (strs.length == 0) throw new Exception("Suffixes must be exist!")
    strs.map(pre.concat(".").concat)
  }

  def main(args: Array[String]) {
    //    val prop = ConfigUtils.getConfig("/config/global.properties")
    //    val array = getProps(prop, "grid.size", "a", "b")
    //    println(array.mkString(","))
    val keyFullname = getKeyFullname("oracle", "url", "driver", "username")
    keyFullname.foreach(println)
  }
}
