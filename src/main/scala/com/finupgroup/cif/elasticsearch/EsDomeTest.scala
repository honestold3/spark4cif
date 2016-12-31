package com.finupgroup.cif.elasticsearch

import com.puhui.aes.AesEncryptionUtil

/**
  * Created by wq on 2016/12/24.
  */
object EsDomeTest extends App{

  val en = AesEncryptionUtil.encrypt("ASC")

  println(s"en:$en")

  val de = AesEncryptionUtil.decrypt(en)

  println(s"de:$de")

}
