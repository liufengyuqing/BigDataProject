package batch.sink.mysql;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

/**
 * create by liuzhiwei on 2020/3/31
 * 往kafka中写数据,可以使用这个main函数进行测试
 */
public class KafkaUtils {
    public static final String broker_list = "localhost:9092";
    public static final String topic = "student";

    public static void producerData() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", broker_list);
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        while (true) {
            int id = new Random().nextInt(100);
            String name = "zhiwei_" + id;
            int age = new Random().nextInt(100);
            String sex = String.valueOf(new Random().nextInt(2));

            Student student = new Student(id, name, age, sex);

            ProducerRecord<String, String> stringProducerRecord = new ProducerRecord<>(topic, JSON.toJSONString(student));
            System.out.println("发送数据：" + JSON.toJSONString(student));
            kafkaProducer.send(stringProducerRecord);

            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    public static void main(String[] args) {
        producerData();
    }
}
