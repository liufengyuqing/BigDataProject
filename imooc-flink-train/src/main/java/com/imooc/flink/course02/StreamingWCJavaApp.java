package com.imooc.flink.course02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @ClassName: StreamingWCJavaApp
 * @Description: 使用Java API来开发Flink的实时处理应用程序
 * @Create by: liuzhiwei
 * @Date: 2020/2/7 8:34 下午
 */

public class StreamingWCJavaApp {
    public static void main(String[] args) throws Exception {

        //step1 ： 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //step2 ： 获取数据 监听本地9999端口 默认是换行符\n分割 一行一行的读入数据
        DataStreamSource<String> text = env.socketTextStream("localhost", 9999);

        //step3：transform  将接收的数据进行拆分，分组，窗口计算并且进行聚合输出
        text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] tokens = value.toLowerCase().split(",");
                for (String token : tokens) {
                    if (token.length() > 0) {
                        collector.collect(new Tuple2<String, Integer>(token, 1));
                    }
                }

            }
        })
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> v1, Tuple2<String, Integer> v2) throws Exception {
                        return new Tuple2<>(v1.f0, v1.f1 + v2.f1);
                    }
                })
                //.sum(1)

                .print()
                .setParallelism(1);

        //step4: 执行
        env.execute("StreamingWCJavaApp");


    }
}
