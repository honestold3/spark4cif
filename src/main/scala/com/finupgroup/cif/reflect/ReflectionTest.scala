package com.finupgroup.cif.reflect

import scala.reflect.runtime.{universe => ru}

/**
  * Created by wq on 2017/5/11.
  */

trait theTrait { def theMethod(x: String): Unit }

// the different logic held in different objects
object object1 extends theTrait {
  def theMethod(x: String) = { println("a " + x ) }
}

object object2 extends theTrait {
  def theMethod(x: String) = { println("b " + x ) }
}

object object3 extends theTrait {
  def theMethod(x: String) = { println("c " + x ) }

  def kankan(i: Int) = i+1
}

// run static/dynamic reflection methods
object ReflectionTest extends App{

  // "static" invocation calling object1.theMethod
  def staticInvocation() = {
    val m = ru.runtimeMirror(getClass.getClassLoader)
    val im = m.reflect(com.finupgroup.cif.reflect.object1)
    val method = ru.typeOf[com.finupgroup.cif.reflect.object1.type].declaration(ru.newTermName("theMethod")).asMethod
    val methodRun = im.reflectMethod(method)
    methodRun("test")
  }

  staticInvocation

  // "dynamic" invocation using integer to call different methods
  def dynamicInvocation( y: Integer) = {
    val m = ru.runtimeMirror(getClass.getClassLoader)
    val module = m.staticModule("com.finupgroup.cif.reflect.object" + y)
    val im = m.reflectModule(module)
    val method = im.symbol.typeSignature.member(ru.newTermName("theMethod")).asMethod

    im.symbol.typeSignature.members.filter(_.isMethod).map{x=>x.toString.split(" ")(1)}.foreach(x=> println(s"x=:::$x"))


    def methodName: PartialFunction[String, String] = {
      case x if x.toString().split(" ")(0)== "method" => x
    }


    val objMirror = m.reflect(im.instance)
    objMirror.reflectMethod(method)("test1")

  }

  //dynamicInvocation(1)
  //dynamicInvocation(2)
  dynamicInvocation(3)

}
