package com.finupgroup.cif.parsers
import scala.util.parsing.combinator._


/**
  * Created by wq on 2017/1/31.
  */
object DigiParser extends RegexParsers{

  def digis = "\\d+".r ^^ (_.toInt)

  def main(args: Array[String]): Unit = {
    val res = DigiParser.parseAll(DigiParser.digis,"333w")

    //import DigiParser.{Success,Failure,Error}

    res match {
      case Success(r,n) => println(r)
      case Failure(msg,n) => println(n.source)
      case Error(msg,n) => println(msg)
    }
  }
}
