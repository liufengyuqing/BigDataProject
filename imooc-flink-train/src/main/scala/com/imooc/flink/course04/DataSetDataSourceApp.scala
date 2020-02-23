package com.imooc.flink.course04

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

/**
 * @ClassName: DataSetDataSourceApp
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/2/8 9:26 上午
 */

object DataSetDataSourceApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    //fromCollection(env)

    // testFile(env)

    //csvFile(env)

    readRecursiveFiles(env)


  }

  import org.apache.flink.api.scala._

  def fromCollection(env: ExecutionEnvironment): Unit = {
    val data = 1 to 10
    env.fromCollection(data).print()

  }

  def testFile(env: ExecutionEnvironment): Unit = {

    // 一个具体的文件
    //    val filePath = "file:///Users/liuzhiwei/data/flink/input/hello.txt"
    //    env.readTextFile(filePath).print()


    //一个文件目录
    val filePath = "file:///Users/liuzhiwei/data/flink/input"
    env.readTextFile(filePath).print()
  }

  def csvFile(env: ExecutionEnvironment): Unit = {
    val filePath = "file:///Users/liuzhiwei/data/flink/input/hello.csv"
    //env.readCsvFile[(String, Int, String)](filePath, ignoreFirstLine = true).print()

    //env.readCsvFile[(String, Int)](filePath, ignoreFirstLine = true).print()
    //env.readCsvFile[(String, Int)](filePath, ignoreFirstLine = true, includedFields = Array(0, 1)).print()

    //    case class MyCaseClass(age: Int, job: String)
    //    env.readCsvFile[MyCaseClass](filePath, ignoreFirstLine = true, includedFields = Array(1, 2)).print()

    env.readCsvFile[Person](filePath, ignoreFirstLine = true, pojoFields = Array("name", "age", "job")).print()

  }

  def readRecursiveFiles(env: ExecutionEnvironment): Unit = {
    val filePath = "file:///Users/liuzhiwei/data/flink/input"
    env.readTextFile(filePath).print()
    println("~~~~~~~~~华丽的分割线~~~~~~~~~~~~")

    // create a configuration object
    val parameters = new Configuration

    // set the recursive enumeration parameter
    parameters.setBoolean("recursive.file.enumeration", true)

    // pass the configuration to the data source
    env.readTextFile(filePath).withParameters(parameters).print()

  }

  /**
   * 读取压缩文件
   *
   * @param env
   */
  def readCompressionFiles(env: ExecutionEnvironment): Unit = {
    val filePath = "file:///Users/liuzhiwei/data/flink/input/compression"
    env.readTextFile(filePath).print()
  }

}
