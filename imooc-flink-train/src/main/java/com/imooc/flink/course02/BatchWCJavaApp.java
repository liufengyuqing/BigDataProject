package com.imooc.flink.course02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/**
 * @ClassName: BatchWCJavaApp
 * @Description: 使用Java API来开发Flink的批处理应用程序
 * @Create by: liuzhiwei
 * @Date: 2020/2/7 3:00 下午
 */

public class BatchWCJavaApp {

    public static void main(String[] args) throws Exception {
        String input = "file:///Users/liuzhiwei/data/flink/input";

        // step1：获取上下文 运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // step2：read data
        DataSource<String> text = env.readTextFile(input);

        // step3: transform operations 开发的核心所在
        //text.print();

        text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] tokens = value.toLowerCase().split("\t");
                for (String token : tokens) {
                    if (token.length() > 0) {
                        collector.collect(new Tuple2<String, Integer>(token, 1));
                    }
                }
            }
        }).groupBy(0).sum(1).print();


    }

}
