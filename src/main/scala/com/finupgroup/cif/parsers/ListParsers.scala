package com.finupgroup.cif.parsers

import scala.util.parsing.combinator.syntactical.StandardTokenParsers

import scala.util.parsing.combinator.syntactical.StdTokenParsers

/**
  * Created by wq on 2016/12/28.
  */
object ListParsers extends StandardTokenParsers {
  lexical.delimiters ++= List("(", ")", ",")

  def expr: Parser[Any] = "(" ~ exprs ~ ")" | ident | numericLit
  def exprs: Parser[Any] = expr ~ rep ("," ~ expr)

  def main(args: Array[String]) {
    val str = "(ddd,eeee,(qq,tt))"
    val tokens = new lexical.Scanner(str)
    println(str)
    println(phrase(expr)(tokens))
  }
}

object ListParsers1 extends StandardTokenParsers {
  lexical.delimiters ++= List("(", ")", ",")

  def expr: Parser[Any] = "(" ~> exprs <~ ")" | ident | numericLit

  def exprs: Parser[List[Any]] = expr ~ rep ("," ~> expr) ^^ { case x ~ y => x :: y }

  def main(args: Array[String]) {
    val str = "(111,ccc,eeee,(qq,tt))"
    val tokens = new lexical.Scanner(str)
    println(str)
    println(phrase(expr)(tokens))
  }
}
