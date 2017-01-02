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
    case (a ~ b ~ d) => d
  }

  def expr2 = rep(factor <~ ",?".r)

  def expr3 = repsep(factor,",")


  val str = """ "asd665" """

  val str1 = """ ("qwe") """

  val str2 = "345"

  val str3 = "1,2,3"

  val result = parseAll(Num,str2)
  println(result)

  val kankan = parseAll(expr1,str1)

  println(s"kankan: $kankan")

  val kk = parseAll(expr3,str3)

  println(kk)

  val kkk = parseAll(expr0,str2)
  println(kkk)

}
