package flink.streaming.topN;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * create by liuzhiwei on 2020/4/27
 * 向Kafka发消息模拟购买事件
 * <p>
 * 需求
 * 某个图书网站，希望看到双十一秒杀期间实时的热销排行榜单。我们可以将“实时热门商品”翻译成程序员更好理解的需求:每隔5秒钟输出最近一小时内点击量最多的前 N 个商品/图书.
 * 需求分解
 * 将这个需求进行分解我们大概要做这么几件事情：
 * 告诉 Flink 框架基于时间做窗口，我们这里用processingTime，不用自带时间戳
 * 过滤出图书点击行为数据
 * 按一小时的窗口大小，每5秒钟统计一次，做滑动窗口聚合（Sliding Window）
 * 聚合，输出窗口中点击量前N名的商品
 */
public class KafkaProducer {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> text = env.addSource(new MyNoParalleSource()).setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        //new FlinkKafkaProducer("topn",new KeyedSerializationSchemaWrapper(new SimpleStringSchema()),properties,FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer("topn", new SimpleStringSchema(), properties);
/*
        //event-timestamp事件的发生时间
        producer.setWriteTimestampToKafka(true);
*/
        text.addSink(producer);
        env.execute();
    }
}
