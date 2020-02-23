package com.imooc.flink.course03

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @ClassName: StreamingWCScalaApp
 * @Description: 使用Scala 开发Flink的实时处理应用程序
 * @Create by: liuzhiwei
 * @Date: 2020/2/7 9:02 下午
 */

object StreamingWCScalaApp {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.socketTextStream("localhost", 9999)

    //引入隐式转换
    import org.apache.flink.api.scala._

    text.flatMap(_.split(","))
      .map(x => WC(x,1))
      .keyBy("word")
      .timeWindow(Time.seconds(5))
      .sum("count")
      .print()
      .setParallelism(1)


    env.execute("StreamingWCScalaApp")


  }

  case class WC(word: String, count: Int)


}
