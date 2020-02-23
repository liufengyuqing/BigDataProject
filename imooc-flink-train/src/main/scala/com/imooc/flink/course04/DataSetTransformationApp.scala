package com.imooc.flink.course04

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ListBuffer

/**
 * @ClassName: DataSetTransformationApp
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/2/8 10:50 上午
 */

object DataSetTransformationApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    //mapFunction(env)

    //filterFunction(env)

    //mapPartitionFunction(env)

    //firstFunction(env)

    //flatMapFunction(env)

    //distinctFuntion(env)

    //joinFunction(env)

    crossFunction(env)

  }

  import org.apache.flink.api.scala._

  def mapFunction(env: ExecutionEnvironment): Unit = {
    val data = env.fromCollection(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    //对data中的每个元素都做一个+1的操作
    //data.map((x: Int) => x + 1).print()
    //data.map((x) => x + 1).print()
    //data.map(x => x + 1).print()
    data.map(_ + 1).print()
  }

  def filterFunction(env: ExecutionEnvironment): Unit = {
    val data = env.fromCollection(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    data.map(_ + 1).filter(_ > 5).print()
  }

  //DataSource 100个元素，把结果存储到数据库中
  def mapPartitionFunction(env: ExecutionEnvironment): Unit = {
    val students = new ListBuffer[String]
    for (i <- 1 to 10) {
      students.append("student: " + i)
    }
    val data = env.fromCollection(students).setParallelism(4)
    //    data.map(x => {
    //      // 每一个元素要存储到数据库中，肯定要获取一个connection
    //
    //      val connection = DBUtils.getConnection()
    //      println(connection + "....")
    //
    //      //todo... 保存数据库DB
    //
    //      DBUtils.returnConnection(connection)
    //
    //    }).print()

    data.mapPartition(x => {
      val connection = DBUtils.getConnection()
      println(connection + "....")
      //todo... 保存数据库DB
      DBUtils.returnConnection(connection)
      x
    }).print()

  }

  def firstFunction(env: ExecutionEnvironment) = {
    val info = ListBuffer[(Int, String)]()
    info.append((1, "hadoop"))
    info.append((1, "spark"))
    info.append((1, "flink"))
    info.append((2, "java"))
    info.append((2, "spring"))
    info.append((3, "linux"))
    info.append((4, "js"))

    val data = env.fromCollection(info)

    //data.first(3).print()

    //data.groupBy(0).first(2).print()

    data.groupBy(0).sortGroup(1, Order.ASCENDING).first(2).print()
    println("---------------")
    data.groupBy(0).sortGroup(1, Order.DESCENDING).first(2).print()
  }

  def flatMapFunction(env: ExecutionEnvironment) = {
    val info = ListBuffer[String]()
    info.append(("hadoop,spark"))
    info.append(("spark,flink"))
    val data = env.fromCollection(info)

    data.print()
    data.map(_.split(",")).print()
    data.flatMap(_.split(",")).print()

    data.flatMap(_.split(",")).map((_, 1)).groupBy(0).sum(1).print()
  }

  def distinctFuntion(env: ExecutionEnvironment) = {
    val info = ListBuffer[String]()
    info.append(("hadoop,spark"))
    info.append(("spark,flink"))
    val data = env.fromCollection(info)
    data.flatMap(_.split(",")).distinct().print()

  }

  def joinFunction(env: ExecutionEnvironment): Unit = {
    val info1 = ListBuffer[(Int, String)]() //编号 名字
    info1.append((1, "pk"))
    info1.append((2, "spark"))
    info1.append((3, "flink"))
    info1.append((4, "java"))

    val info2 = ListBuffer[(Int, String)]() //编号 名字
    info2.append((1, "北京"))
    info2.append((2, "上海"))
    info2.append((3, "成都"))
    info2.append((5, "杭州"))

    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)

    data1.join(data2).where(0).equalTo(0).apply((first, sencod) => {
      (first._1, sencod._1, sencod._2)
    }).print()

    data1.leftOuterJoin(data2).where(0).equalTo(0).apply((first, sencod) => {
      if (sencod == null) {
        (first._1, first._2, "-")
      } else {
        (first._1, sencod._1, sencod._2)
      }

    }).print()

    data1.rightOuterJoin(data2).where(0).equalTo(0).apply((first, sencod) => {
      if (first == null) {
        (sencod._1, "-", sencod._2)
      } else {
        (sencod._1, first._1, sencod._2)
      }
    }).print()

    data1.fullOuterJoin(data2).where(0).equalTo(0).apply((first, sencod) => {
      if (first == null) {
        (sencod._1, "-", sencod._2)
      } else if (sencod == null) {
        (first._1, first._2, "-")
      } else {
        (sencod._1, first._1, sencod._2)
      }
    }).print()
  }

  /**
   * 笛卡尔积
   * @param env
   */
  def crossFunction(env: ExecutionEnvironment): Unit = {
    val info1 = List("曼联", "曼城")
    val info2 = List(3, 1, 0)

    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)

    data1.cross(data2).print()

  }

}
