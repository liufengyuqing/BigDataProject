package com.imooc.scala

/**
 * @ClassName: HelloWorld
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/2/11 12:31 上午
 */

object HelloWorld {
  def main(args: Array[String]): Unit = {
    println("Hello world")
    println(3 max 5)
    println(3 + 5)
    println(3.+(5))
    val x = 6

    /**
     * if条件表达式，可以fuzhi 给变量
     */
    if (x > 6) {
      1
    } else {
      -1
    }
    val a = if (x > 6) {
      1
    } else {
      -1
    }

    println("a: " + a)

    var i = 9;
    while (i > 0) {
      i -= 1
      printf("i is %d\n", i)

    }

    do {
      i += 1
      print(i)
    } while (i < 5)

    println()

    for (i <- 1 to 5 by 2 if i % 2 != 0) {
      println(i)
    }

    for (i <- 1 to 5; j <- 1 to 3) {
      println(i * j)
    }

    var r = for (i <- Array(1, 2, 3, 4, 5) if i % 2 == 0) yield {
      println(i); i
    }

    val intVauleArr = new Array[Int](3)
    intVauleArr(0) = 1
    intVauleArr(2) = 2
    intVauleArr(1) = 1

    //二维数组 3行4列
    val myMatrix = Array.ofDim(3, 4)

    //scala 没有接口 特质





  }

}
