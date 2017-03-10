package com.finupgroup.cif.utils

/**
  * Created by wq on 2017/3/10.
  */
object RegexUtils extends App{

  val re = "du".r

  re.findAllIn("duoodu").toList.foreach(println)

  val kk = re.findPrefixOf("dukkkdu du")

  val list = re.findAllMatchIn("dud").toList
  list.map(_.source)

  val list1 = re.findAllIn("du,,,").toList
  println(s"kk:$kk")

}
