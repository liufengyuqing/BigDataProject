package com.imooc.flink.course05

import java.{lang, util}

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.collector.selector.OutputSelector

/**
 * @ClassName: DataStreamTransformApp
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/2/8 10:46 下午
 */

object DataStreamTransformApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //filterFunction(env)

    //unionFunction(env)

    splitSelectFunction(env)

    env.execute("DataStreamTransformApp")
  }


  def splitSelectFunction(env: StreamExecutionEnvironment): Unit = {
    val data = env.addSource(new CustomNonParallelSourceFunction)
    val splits = data.split(new OutputSelector[Long] {
      override def select(value: Long): lang.Iterable[String] = {
        val list = new util.ArrayList[String]()
        if (value % 2 == 0) {
          list.add("even")
        } else {
          list.add("odd")
        }
        list
      }
    })
    splits.select("odd").print().setParallelism(1)
  }

  def unionFunction(env: StreamExecutionEnvironment): Unit = {
    val data1 = env.addSource(new CustomNonParallelSourceFunction)
    val data2 = env.addSource(new CustomNonParallelSourceFunction)

    data1.union(data2).print().setParallelism(1)
  }

  def filterFunction(env: StreamExecutionEnvironment): Unit = {
    val data = env.addSource(new CustomNonParallelSourceFunction)
    data.map(x => {
      println("receive" + x)
      x
    }).filter(_ % 2 == 0).print().setParallelism(1)
  }
}
