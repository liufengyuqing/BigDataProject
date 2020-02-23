package com.imooc.flink.course03;

/**
 * @ClassName: SocketWindowWordCountApp
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/2/10 10:15 下午
 */


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * create by liuzhiwei on 2019/11/23
 * 例子
 * https://ci.apache.org/projects/flink/flink-docs-release-1.9/getting-started/tutorials/local_setup.html
 */
public class SocketWindowWordCountApp {
    public static void main(String[] args) throws Exception {
        //定义socket的端口号 the port to connect to
        int port;
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        } catch (Exception e) {
            System.err.println("No port set. use default port 9000--Java");
            port = 9000;
        }
        // get the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // get input data by connecting to the socket
        DataStreamSource<String> text = env.socketTextStream("localhost", port, "\n");

        DataStream<WordWithCount> windowCounts = text.flatMap(new FlatMapFunction<String, WordWithCount>() {
            @Override
            public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                for (String word : value.split("\\s")) {
                    out.collect(new WordWithCount(word, 1L));
                }
            }

        }).keyBy("word")//针对相同的word数据进行分组
                .timeWindow(Time.seconds(1), Time.seconds(1))// //指定计算数据的窗口大小和滑动窗口大小
                //.sum("count");
                .reduce(new ReduceFunction<WordWithCount>() {
                    @Override
                    public WordWithCount reduce(WordWithCount a, WordWithCount b) throws Exception {
                        return new WordWithCount(a.word, a.count + b.count);
                    }
                });
        // print the results with a single thread, rather than in parallel
        //把数据打印到控制台
        windowCounts.print().setParallelism(1);//使用一个并行度
        //注意：因为flink是懒加载的，所以必须调用execute方法，上面的代码才会执行
        env.execute("Socket Window WordCount");
    }
    // Data type for words with count

    /**
     * 主要为了存储单词以及单词出现的次数
     */
    public static class WordWithCount {
        public String word;
        public long count;

        public WordWithCount() {
        }

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}

