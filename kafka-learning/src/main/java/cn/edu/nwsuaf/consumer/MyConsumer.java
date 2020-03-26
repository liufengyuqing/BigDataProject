package cn.edu.nwsuaf.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @ClassName: MyConsumer
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/2/20 12:42 下午
 */

public class MyConsumer {
    public static void main(String[] args) {

        //1.创建消费者配置信息
        Properties props = new Properties();

        //2.链接的集群
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop000:9092");
        //3.开启自动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        //4.自动提交的延时
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        //5.key value 的反序列化
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        //6.消费者组
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "bigdata-20200220");

        //重置消费者offset
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        //创建消费者主题

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        //订阅主题topic
        consumer.subscribe(Arrays.asList("first", "second"));

        //获取数据
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100);

            //解析并打印consumerRecords
            for (ConsumerRecord consumerRecord : consumerRecords) {
                System.out.println(consumerRecord.key() + " " + consumerRecord.value());
            }

        }
        //关闭链接
        //consumer.close();
    }
}
