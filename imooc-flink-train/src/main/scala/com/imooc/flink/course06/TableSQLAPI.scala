package com.imooc.flink.course06

import org.apache.flink.api.java.DataSet
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

/**
 * @ClassName: TableSQLAPI
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/2/9 3:25 下午
 */

object TableSQLAPI {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val filePath = "/Users/liuzhiwei/data/flink/input/table_sql_sales.csv"

    //已经拿到DataSet
    val csv = env.readCsvFile[SalesLog](filePath, ignoreFirstLine = true)

    //csv.print()

    //DataSet => table
    val salesTable = tableEnv.fromDataSet(csv)

    //table => table
    tableEnv.registerTable("sales", salesTable)

    val resultTable = tableEnv.sqlQuery("select customerId,sum(amount) amount from sales group by customerId ")

    tableEnv.toDataSet[Row](resultTable).print()


  }

  case class SalesLog(orderId: String, customerId: String, itemId: String, amount: Double)

}
