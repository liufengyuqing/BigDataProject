package batch.sink.mysql;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * create by liuzhiwei on 2020/3/31
 */
public class FlinkProcess {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "student");
        properties.setProperty("auto.offset.reset", "earliest");

        String topic = "student";

        /**
         * 实现Student序列化接口DeserializationSchema
         */
        //FlinkKafkaConsumer<Student> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new StudentDeserializationSchema(), properties);
        //streamSource.addSink(new MySqlSink());


        /**
         * 先使用String解析 再通过fastJson解析成Student对象
         */
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);
        DataStreamSource<String> streamSource = env.addSource(kafkaConsumer);
        SingleOutputStreamOperator<Student> mapStudent = streamSource.map(new MapFunction<String, Student>() {
            @Override
            public Student map(String value) throws Exception {
                Student student = JSON.parseObject(value, Student.class);
                return student;
            }
        });
        mapStudent.print("消费的student");

        mapStudent.addSink(new MySqlSink());


        env.execute("FlinkProcess");

    }
}
