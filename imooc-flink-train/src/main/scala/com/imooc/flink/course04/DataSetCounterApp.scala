package com.imooc.flink.course04

import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
 * @ClassName: CounterApp
 * @Description: 基于flink 计数器开发流程
 *               // step1 定义计数器
 *               // step2：注册计数器
 *               // step3 获取计数器
 * @Create by: liuzhiwei
 * @Date: 2020/2/8 6:37 下午
 */

object DataSetCounterApp {

  import org.apache.flink.api.scala._

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val data = env.fromElements("hadoop", "spark", "flink", "pyspark", "storm")

    //    data.map(new RichMapFunction[String, Long]() {
    //      var counter = 0l
    //
    //      override def map(in: String): Long = {
    //        counter = counter + 1
    //        println("counter：" + counter)
    //        counter
    //      }
    //    }).setParallelism(4).print()

    val info = data.map(new RichMapFunction[String, String]() {

      // step1 定义计数器
      var counter = new LongCounter()

      override def open(parameters: Configuration): Unit = {
        // step2：注册计数器
        getRuntimeContext.addAccumulator("ele-counts-scala", counter)
      }

      override def map(value: String): String = {
        counter.add(1)
        value
      }
    })

    //info.print()

    val filePath = "file:///Users/liuzhiwei/data/flink/input/sink-out-counter-scala"
    info.writeAsText(filePath, WriteMode.OVERWRITE).setParallelism(5)

    // step3 获取计数器
    val jobResult = env.execute("DataSetCounterApp")
    val num = jobResult.getAccumulatorResult[Long]("ele-counts-scala")
    println("num: " + num)

  }

}
