package com.imooc.flink.course07

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @ClassName: WindowsApp
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/2/9 5:02 下午
 */

object WindowsApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost", 9999)

    text.flatMap(_.split(","))
      .map((_, 1))
      .keyBy(0)
      //.countWindow()
      //.timeWindow(Time.seconds(5))
      .timeWindow(Time.seconds(10), Time.seconds(5))
      .sum(1)
      .print()
      .setParallelism(1)


    env.execute("WindowsApp")
  }

}
