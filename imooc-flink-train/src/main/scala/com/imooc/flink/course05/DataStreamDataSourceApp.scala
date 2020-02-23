package com.imooc.flink.course05

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import org.apache.flink.api.scala._

/**
 * @ClassName: DataStreamDataSourceApp
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/2/8 9:44 下午
 */

object DataStreamDataSourceApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //socketFunctio(env)

    //nonParallelSourceFunction(env)

    //parallelSourceFunction(env)

    customRichParallelSourceFunction(env)

    env.execute("DataStreamDataSourceApp")

  }

  def customRichParallelSourceFunction(env: StreamExecutionEnvironment): Unit = {
    val data = env.addSource(new CustomRichParallelSourceFunction).setParallelism(2)

    data.print()

  }

  def parallelSourceFunction(env: StreamExecutionEnvironment): Unit = {
    val data = env.addSource(new CustomParallelSourceFunction).setParallelism(2)
    data.print().setParallelism(1)
  }

  def nonParallelSourceFunction(env: StreamExecutionEnvironment): Unit = {
    val data = env.addSource(new CustomNonParallelSourceFunction)
    data.print().setParallelism(1)
  }

  def socketFunctio(env: StreamExecutionEnvironment): Unit = {
    val data = env.socketTextStream("localhost", 9999)

    data.print().setParallelism(1)
  }

}
