package com.finupgroup.cif.recursion

import com.finupgroug.cif.utils.StopWatch
//import com.google.common.base.Stopwatch
//import org.apache.commons.lang.time.StopWatch

import scala.annotation.tailrec


/**
  * Created by wq on 2017/2/4.
  * 最大的区别：递归方法在递归调用后还需要进行一次“+1”，而尾递归的递归调用属于方法的最后一个操作。这就是所谓的“尾递归”
  * 与普通递归相比，由于尾递归的调用处于方法的最后，因此方法之前所积累下的各种状态对于递归调用结果已经没有任何意义，
  * 因此完全可以把本次方法中留在堆栈中的数据完全清除，把空间让给最后的递归调用。这样的（编译器）优化便使得递归不会在调用堆栈上产生堆积，
  * 意味着即使是“无限”递归也不会让堆栈溢出。这便是尾递归的优势。
  */
object tail  extends App{

  def fibonacci(n: Int): Int = {
    if (n <= 2) 1
    else fibonacci(n - 1) + fibonacci(n - 2)
  }

  // fibonacci(5)
  // fibonacci(4) + fibonacci(3)
  // (fibonacci(3) + fibonacci(2)) + (fibonacci(2) + fibonacci(1))
  // ((fibonacci(2) + fibonacci(1)) + 1) + (1 + 1)
  // ((1 + 1) + 1) + 2
  // 5

  @tailrec
  def fibonacciTailrec(n: Int, acc1: Int, acc2: Int): Int = {
    if (n < 2) acc2
    else fibonacciTailrec(n - 1, acc2, acc1 + acc2)
  }
  // fibonacciTailrec(5,0,1)
  // fibonacciTailrec(4,1,1)
  // fibonacciTailrec(3,1,2)
  // fibonacciTailrec(2,2,3)
  // fibonacciTailrec(1,3,5)
  // 5



  val list = List(30, 40, 45)
  val sw = new StopWatch
  for (num <- list) {
    println("n = " + num)
    sw.start("Normal")
    //sw.start()
    val ret = fibonacci(num)
    //val ret = gcd(42,21)
    println("F(n) = " + ret)
    sw.stop()

    sw.start("Tail")
    //sw.start
    val retTail = fibonacciTailrec(num, 0, 1)
    println("FT(n)  = " + retTail)
    sw.stop()
    println(sw.prettyPrint())
//    println(sw.getTime)

    sw.reset()
  }

}
