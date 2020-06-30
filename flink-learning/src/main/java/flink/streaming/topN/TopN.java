package flink.streaming.topN;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * create by liuzhiwei on 2020/4/27
 * <p>
 * 需求
 * <p>
 * 某个图书网站，希望看到双十一秒杀期间实时的热销排行榜单。
 * 我们可以将“实时热门商品”翻译成程序员更好理解的需求:
 * 每隔5秒钟输出最近一小时内点击量最多的前 N 个商品/图书.
 * <p>
 * 需求分解
 * 将这个需求进行分解我们大概要做这么几件事情：
 * 告诉 Flink 框架基于时间做窗口，我们这里用processingTime，不用自带时间戳
 * 过滤出图书点击行为数据
 * 按一小时的窗口大小，每5秒钟统计一次，做滑动窗口聚合（Sliding Window）
 * 聚合，输出窗口中点击量前N名的商品
 */
public class TopN {
    public static void main(String[] args) throws Exception {
        //每隔5秒钟 计算过去1小时 的 Top 3 商品
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "localhost:9092");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("topn", new SimpleStringSchema(), properties);
        //从最早开始消费 位点
        kafkaConsumer.setStartFromEarliest();

        DataStreamSource<String> stream = env.addSource(kafkaConsumer);

        //将输入语句split成一个一个单词并初始化count值为1的Tuple2<String, Integer>类型
        SingleOutputStreamOperator<Tuple2<String, Integer>> ds = stream.flatMap(new LineSplitter());

        //key之后的元素进入一个总时间长度为600s,每5s向后滑动一次的滑动窗口
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCounts = ds
                .keyBy(0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(600), Time.seconds(5)))
                // 将相同的key的元素第二个count值相加
                .sum(1);

        //所有key元素进入一个5s长的窗口（选5秒是因为上游窗口每5s计算一轮数据，topN窗口一次计算只统计一个窗口时间内的变化）
        wordCounts.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new TopNAllFunction(3))
                .print();


        //redis sink  redis -> 接口
        env.execute();

    }

}
