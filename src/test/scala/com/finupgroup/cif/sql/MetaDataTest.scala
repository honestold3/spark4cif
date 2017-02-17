package com.finupgroup.cif.sql

/**
 * Created by jadetang on 15-3-29.
 */
class MetaDataTest {
  import MetaData._
  import TestData._

  @Test
  def testMaxAndMin = {
    val x = user1.maxBy(row=>row("age")).collect{ case("age",_2)=>("xxx",_2)}
    val m:Row = Map(x.head._1->x.head._2)
    println(user1.maxBy(row=>row("age")))
    println(user1.minBy(row=>row("age")))
  }

  @Test
  def testSum = {
    println(user1 map(_("age")) sum)
  }

  @Test
  def orderBy = {
    printTable(user1)
    val keys = Seq("age","name")
    printTable(user1.sortBy(row=>keys.map(row(_))))
  }

}
