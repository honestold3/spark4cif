package com.finupgroup.cif.specs2

import org.specs2.mutable._

/**
  * Created by wq on 2017/5/5.
  */
class HelloWorldUnitSpec extends Specification {

  "HelloWorldUnit" should {
    "contain 11 characters" in {
      "Hello world" must have size(11)
    }
    "start with 'Hello'" in {
      "Hello world" must startWith("Hello")
    }
    "end with 'world'" in {
      "Hello world" must endWith("world")
    }
  }
}
