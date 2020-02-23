package com.imooc.flink.course03

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @ClassName: ScalaSocketWindowWordCountApp
 * @Description: 需求：实现每隔1s对最近2s内的数据进行汇总计算
 * @Create by: liuzhiwei
 * @Date: 2020/2/12 8:46 上午
 */

/**
 * 单词计数之滑动窗口计算
 */
object ScalaSocketWindowWordCountApp {
  def main(args: Array[String]): Unit = {
    //获取socket口号
    val port: Int =
      try {
        ParameterTool.fromArgs(args).getInt("port")
      } catch {
        case e: Exception => {
          println("No port set. use default port 9000--Java No port specified. Please run 'SocketWindowWordCount --port <port>\"")
        }
          9000
      }

    //获取运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //获取socket输入数据
    val text = env.socketTextStream("localhost", port)

    //解析数据 把数据打平 分组 窗口计算 并且聚合求和
//    val wcCounts = text.flatMap(line => line.split(" "))
//      .map(w => (w, 1))
//      .keyBy(0)
//      .timeWindow(Time.seconds(2), Time.seconds(1))
//      .sum(1)


    //解析数据 把数据打平 分组 窗口计算 并且聚合求和
    val wcCounts = text.flatMap(line => line.split(" "))
      .map(w => WordWithCount(w, 1))
      .keyBy("word")
      .timeWindow(Time.seconds(2), Time.seconds(1))
      .reduce((a, b) => WordWithCount(a.word, a.count + b.count))

    //打印到控制台
    wcCounts.print().setParallelism(1)

    //执行任务
    env.execute("ScalaSocketWindowWordCountApp")

  }


}

case class WordWithCount(word: String, count: Long)