package com.finupgroup.cif.singleton

/**
  * Created by wq on 2017/8/22.
  */
class Worker private{

  var dbMap: Map[String,String] = Map()

  def work() = {
    println("I am the only worker!")
  }

  def setMap(a: (String,String)): Unit ={
    dbMap += a
  }
}

object Worker{
  val worker = new Worker
  def GetWorkInstance()  = {
    worker.work()
    worker
  }
}

object Job{
  def main(args: Array[String]) {
    for (i <- 1 to 5) {
      var kankan = Worker.GetWorkInstance()
      println(kankan)
      println(kankan.dbMap)
      kankan.setMap(("ddd"->s"fff$i"))
    }
    val kankan = Worker.GetWorkInstance()
    println(kankan.dbMap.size)
    println(kankan.dbMap.getOrElse("ddddd","notuse"))
  }
}
