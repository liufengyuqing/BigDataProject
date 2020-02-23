package com.imooc.flink.course04;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;


/**
 * @ClassName: JavaDataSetCounterApp
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/2/8 8:43 下午
 */

public class JavaDataSetCounterApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> data = env.fromElements("hadoop", "spark", "flink", "pyspark", "storm");

        DataSet<String> info = data.map(new RichMapFunction<String, String>() {

            //step1 定义累加器
            LongCounter counter = new LongCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                // step2：注册计数器
                getRuntimeContext().addAccumulator("ele-counts-java", counter);
            }

            @Override
            public String map(String value) throws Exception {
                counter.add(1);
                return value;
            }
        });

        String filePath = "file:///Users/liuzhiwei/data/flink/input/sink-out-counter-java";
        info.writeAsText(filePath, FileSystem.WriteMode.OVERWRITE).setParallelism(3);
        JobExecutionResult jobExecutionResult = env.execute("JavaDataSetCounterApp");

        //step3 获取计数器
        long num = jobExecutionResult.getAccumulatorResult("ele-counts-java");
        System.out.println("num:" + num);
    }
}
