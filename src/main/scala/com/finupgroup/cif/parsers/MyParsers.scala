package com.finupgroup.cif.parsers

import scala.util.parsing.combinator.JavaTokenParsers

/**
  * Created by wq on 2016/12/31.
  */
object MyParsers extends App with JavaTokenParsers{

  def Num: Parser[Double] = floatingPointNumber ^^ (_.toDouble)

  def Str: Parser[String] = stringLiteral ^^ (_.toString)

  def expr  = "("~> factor <~")" ^^ (x=>x)

  def factor = Num | Str

  def expr0 = opt(factor)

  def expr1 = "(" ~ factor ~ ")" ^^ {
    case (a ~ b ~ d) => b
  }

  def expr2 = rep(factor <~ ",?".r)

  def expr3 = repsep(factor,",")

  def expr4 = rep(factor ~ ",?".r)

  def expr5 = rep1sep(factor,",")

  def kkkk = factor ^^^ (println("fff"))

  def kankanemp = rep1sep(factor,"")^^{

    case a => {
      val lb = new scala.collection.mutable.ListBuffer[Any]
//      val sb = new java.lang.StringBuilder
//      a.foreach{sb.append(_)}
//
//      val k = for{
//        i<- a
//        //lb + x.asInstanceOf[String]
//      } yield {i}
//      k
//      sb.toString
      val kkk = a.map{
        case x: String =>
          x.toCharArray
      }
      val lens = for(i<-kkk) yield (i.length,kkk)
      val data = lens.map(_._2).foreach(_.flatten)
      println("dddd:"+data)
      val len = lens.map(_._1).reduce(_+_)
      len
    }


  }


  val str = """ "asd665" """

  val str1 = """ ("qwe") """

  val str2 = "345"

  val str3 = "1,2,3"

  val str4 = "1234"

  val str5 = """  "ddd""ffff"  """

  val result = parseAll(Num,str2)
  println(result)

  val kankan = parseAll(expr,str1)

  println(s"kankan: $kankan")

  val kk = parseAll(expr3,str2)

  println("expr2::::"+kk)

  val he = parseAll(expr4,str3)

  println("expr4::::"+he)

  val kkk = parseAll(expr0,str4)
  println(kkk)

  val ex5 = parseAll(expr5,str2)
  println(s"expr5::::$ex5")

  val kankan1 = parseAll(kkkk,"1111")
  println(kankan1)

  val empty = parseAll(kankanemp,str5)
  println(empty)

}
