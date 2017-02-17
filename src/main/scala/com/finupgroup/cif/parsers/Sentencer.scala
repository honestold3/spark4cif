package com.finupgroup.cif.parsers

import scala.util.parsing.combinator.syntactical.StandardTokenParsers

/**
  * Created by wq on 2016/12/31.
  */
object Sentencer extends StandardTokenParsers {

  def sentence: Parser[Any] = "(" ~> sentence <~")" | ident | numericLit

  def main(args: Array[String]): Unit = {

    val str = "(123)"

    println(phrase(sentence)(new lexical.Scanner("""("111")""")))

  }

}
