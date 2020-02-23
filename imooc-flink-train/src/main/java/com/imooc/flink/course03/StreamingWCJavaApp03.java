package com.imooc.flink.course03;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @ClassName: StreamingWCJavaApp
 * @Description: 使用Java API来开发Flink的实时处理应用程序
 * wc 统计的数据我们源自于socket
 * @Create by: liuzhiwei
 * @Date: 2020/2/7 8:34 下午
 */

public class StreamingWCJavaApp03 {
    public static void main(String[] args) throws Exception {

        //获取参数
        int port = 0;

        try {
            ParameterTool tool = ParameterTool.fromArgs(args);
            port = tool.getInt("port");
        } catch (Exception e) {
            System.err.println("端口未设置，使用默认端口9999");
            port = 9999;
        }

        //step1 ： 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //step2 ： 获取数据
        DataStreamSource<String> text = env.socketTextStream("localhost", port);

        //step3：transform
        text.flatMap(new FlatMapFunction<String, WC>() {
            @Override
            public void flatMap(String value, Collector<WC> collector) throws Exception {
                String[] tokens = value.toLowerCase().split(",");
                for (String token : tokens) {
                    if (token.length() > 0) {
                        collector.collect(new WC(token.trim(), 1));
                    }
                }

            }
        })
               /* .keyBy("word")
                .timeWindow(Time.seconds(5))
                .sum("count").print()
                .setParallelism(1);*/
               .keyBy(new KeySelector<WC, String>() {
                   @Override
                   public String getKey(WC wc) throws Exception {
                       return wc.word;
                   }
               }).timeWindow(Time.seconds(5))
                .sum("count").print()
                .setParallelism(1);





//        text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
//            @Override
//            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exwception {
//                String[] tokens = value.toLowerCase().split(",");
//                for (String token : tokens) {
//                    if (token.length() > 0) {
//                        collector.collect(new Tuple2<String, Integer>(token, 1));
//                    }
//                }
//
//            }
//        }).keyBy(0).timeWindow(Time.seconds(5)).sum(1).print().setParallelism(1);

        //step4: 执行
        env.execute("StreamingWCJavaApp");


    }

    public static class WC {
        private String word;
        private int count;

        public WC() {
        }

        public WC(String word, int count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WC{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }
    }
}
