package com.finupgroup.cif.parsers

import scala.util.parsing.combinator.syntactical.StandardTokenParsers

/**
  * Created by wq on 2016/12/29.
  */
object ArithmeticParsers extends StandardTokenParsers {
  lexical.delimiters ++= List("(", ")", "+", "-", "*", "/")

  def expr: Parser[Any] = term ~ rep("+" ~ term | "-" ~ term)
  def term = factor ~ rep("*" ~ factor | "/" ~ factor)
  def factor: Parser[Any] = "(" ~ expr ~ ")" | numericLit

  def main(args: Array[String]) {
    val str = "(2+10-(2*3))"
    val tokens = new lexical.Scanner(str)
    println(str)
    println(phrase(expr)(tokens))
  }
}

object ArithmeticParsers1 extends StandardTokenParsers {
  lexical.delimiters ++= List("(", ")", "+", "-", "*", "/")

  val reduceList: Int ~ List[String ~ Int] => Int = {
    case i ~ ps => (i /: ps)(reduce)
  }

  def reduce(x: Int, r: String ~ Int) = (r: @unchecked) match {
    case "+" ~ y => x + y
    case "-" ~ y => x - y
    case "*" ~ y => x * y
    case "/" ~ y => x / y
  }

  def expr  : Parser[Int] = term ~ rep ("+" ~ term | "-" ~ term) ^^ reduceList
  def term  : Parser[Int] = factor ~ rep ("*" ~ factor | "/" ~ factor) ^^ reduceList
  def factor: Parser[Int] = "(" ~> expr <~ ")" | numericLit ^^ (_.toInt)

  def main(args: Array[String]) {
    val str = "(2+10)"
    val tokens = new lexical.Scanner(str)
    println(str)
    println(phrase(expr)(tokens))
  }
}

class Expr
case class Op(op: String, l: Expr, r: Expr) extends Expr
case class Num(n: Int) extends Expr

object ArithmeticParsers2 extends StandardTokenParsers {
  lexical.delimiters ++= List("(", ")", "+", "-", "*", "/")

  val reduceList: Expr ~ List[String ~ Expr] => Expr = {
    case i ~ ps => (i /: ps)(reduce)
  }

  def reduce(l: Expr, r: String ~ Expr) = Op(r._1, l, r._2)
  def mkNum(s: String) = Num(s.toInt)

  def expr  : Parser[Expr] = term ~ rep ("+" ~ term | "-" ~ term) ^^ reduceList
  def term  : Parser[Expr] = factor ~ rep ("*" ~ factor | "/" ~ factor) ^^ reduceList
  def factor: Parser[Expr] = "(" ~> expr <~ ")" | numericLit ^^ ((s: String) => Num(s.toInt))

  def arithmetic(expr: Expr): Int = expr match {
    case Num(i) => i
    case Op("+",x,y) => arithmetic(x)+arithmetic(y)
    case Op("-",x,y) => arithmetic(x)-arithmetic(y)
    case Op("*",x,y) => arithmetic(x)*arithmetic(y)
    case Op("/",x,y) => arithmetic(x)/arithmetic(y)
  }

  def main(args: Array[String]) {
    val str = "(1+2)+(2*3+7)"
    //val str = "(1+2)"
    val parse = phrase(expr)
    val tokens = new lexical.Scanner(str)
    println(str)
    //println(parse(tokens))
    val kankan = parse(tokens).get
    println(kankan)
    println(arithmetic(kankan))
  }
}
