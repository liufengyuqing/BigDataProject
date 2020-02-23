package com.imooc.flink.course04

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration


/**
 * @ClassName: DatasetDistributedApp
 * @Description:
 *
 * step1：注册一个本地HDFS文件
 * step2: 在open方法中获取分布式缓存的内容即可
 * @Create by: liuzhiwei
 * @Date: 2020/2/8 9:06 下午
 */

object DatasetDistributedApp {

  import org.apache.flink.api.scala._

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val filePath = "file:///Users/liuzhiwei/data/hello.txt"

    //step1：注册一个本地HDFS文件
    env.registerCachedFile(filePath, "pk-scala-dc")

    val data = env.fromElements("hadoop", "spark", "flink", "pyspark", "storm")

    data.map(new RichMapFunction[String, String]() {
      override def open(parameters: Configuration): Unit = {
        // step2: 在open方法中获取分布式缓存的内容即可
        val myFile = getRuntimeContext.getDistributedCache.getFile("pk-scala-dc")

        val lines = FileUtils.readLines(myFile)

        /**
         * 此时会出现一个异常：Java集合和Scala集合不兼容的问题
         */
        import scala.collection.JavaConversions._
        for (ele <- lines) { // scala
          println(ele)
        }

      }


      override def map(value: String): String = {
        value
      }
    }).print()


  }

}
