package com.imooc.flink.course06;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @ClassName: JavaTableSQLAPI
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/2/9 4:07 下午
 */

public class JavaTableSQLAPI {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.getTableEnvironment(env);

        String filePath = "/Users/liuzhiwei/data/flink/input/table_sql_sales.csv";
        DataSet<Sales> csv = env.readCsvFile(filePath).ignoreFirstLine().pojoType(Sales.class, "orderId", "customerId", "itemId", "amount");

        //csv.print();

        Table table = tableEnv.fromDataSet(csv);
        tableEnv.registerTable("sales", table);

        Table resultTable = tableEnv.sqlQuery("select customerId,sum(amount) amount from sales group by customerId");

        DataSet<Row> result = tableEnv.toDataSet(resultTable, Row.class);
        result.print();


    }

    public static class Sales {
        public String orderId;
        public String customerId;
        public String itemId;
        public Double amount;
    }
}
