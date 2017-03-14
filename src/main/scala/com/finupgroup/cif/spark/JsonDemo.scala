package com.finupgroup.cif.spark

import org.json4s.JsonDSL.WithDouble._
import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
  * Created by wq on 2016/11/29
  */
object JsonDemo extends App{

  val t=parse(""" { "name" : "tom","age":23,"number":[1,2,3,4] } """)
  println(t)

  for((k,v)<-t.values.asInstanceOf[Map[String,_]]){
    println(k,"->",v,v.getClass)
  }

  println("------------------------------------------------")

  println("name",(t \ "name").values)
  println("age",(t \ "age").values)
  println("number",(t \ "number").values)

  println("------------------------------------------------")

  println(compact(render(List(1,2,3,4))))
  //结果：[1,2,3,4]
  println(compact(render(Map("name"->"tom","age"->"23"))))
  //结果：{"name":"tom","age":"23"}

  println("-------------------------------------------------")
  case class Winner(id: Long, numbers: List[Int])
  case class Lotto(id: Long, winningNumbers: List[Int], winners: List[Winner], drawDate: Option[java.util.Date])

  val winners = List(Winner(23, List(2, 45, 34, 23, 3, 5)), Winner(54, List(52, 3, 12, 11, 18, 22)))
  val lotto = Lotto(5, List(2, 45, 34, 23, 7, 5, 3), winners, None)

  val json =
    ("lotto" ->
      ("lotto-id" -> lotto.id) ~
        ("winning-numbers" -> lotto.winningNumbers) ~
        ("draw-date" -> lotto.drawDate.map(_.toString)) ~
        ("winners" ->
          lotto.winners.map { w =>
            (("winner-id" -> w.id) ~
              ("numbers" -> w.numbers))}))

  println(compact(render(json)))

  println("------------------------------------------------")

  println(pretty(render(json)))



}
