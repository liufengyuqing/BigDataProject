package com.imooc.flink.course04

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
 * @ClassName: DataSetSinkApp
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/2/8 6:21 下午
 */

object DataSetSinkApp {

  import org.apache.flink.api.scala._

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment


    val data = 1.to(10)
    val text = env.fromCollection(data)

    val filePath = "file:///Users/liuzhiwei/data/flink/input/sink-out"
    text.writeAsText(filePath, WriteMode.OVERWRITE).setParallelism(2)

    env.execute("DataSetSinkApp")

  }

}
