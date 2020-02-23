package com.imooc.flink.scala.course02

import org.apache.flink.api.scala.ExecutionEnvironment

/**
 * @ClassName: BatchWCScalaApp
 * @Description: 使用Scala 开发Flink的批处理应用程序
 * @Create by: liuzhiwei
 * @Date: 2020/2/7 8:23 下午
 */

object BatchWCScalaApp {

  def main(args: Array[String]): Unit = {

    val input = "file:///Users/liuzhiwei/data/flink/input"

    val env = ExecutionEnvironment.getExecutionEnvironment

    val text = env.readTextFile(input)

    //text.print()

    //引入隐式转换
    import org.apache.flink.api.scala._

    // todo 1 参考Scala课程 2 学习Scala API
    // 算子 简洁性
    //
    text.flatMap(_.toLowerCase()
      .split("\t"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .groupBy(0)
      .sum(1).print()
  }

}
