package com.imooc.scala.course03

/**
 * @ClassName: FunctionApp
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/2/11 12:38 上午
 */

object FunctionApp {
  def main(args: Array[String]): Unit = {
    //    println(add(2, 3))
    //    println(three())
    //    println(three)
    //    println(sayHello())
    //    sayHello()
    sayName()
  }

  def add(x: Int, y: Int): Int = {
    x + y
  }

  def three() = {
    1 + 2
  }

  def sayHello(): Unit = {
    println("hello world")
  }

  def sayName(name: String = "PK"): Unit = {
    println(name)
  }
}
