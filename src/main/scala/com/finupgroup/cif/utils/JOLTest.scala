package com.finupgroup.cif.utils

import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.vm.VM;

/**
  * Created by wq on 2017/1/13.
  */
object JOLTest extends App{

  val list = List("ddd","eeee")
  println(VM.current.details)
  println(ClassLayout.parseClass("asdf".getClass).toPrintable)
  println(ClassLayout.parseClass(list.getClass).toPrintable)
}
