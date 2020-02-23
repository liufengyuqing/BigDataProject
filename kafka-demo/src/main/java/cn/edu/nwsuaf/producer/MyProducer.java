package cn.edu.nwsuaf.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @ClassName: MyProducer
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/2/19 11:59 下午
 */

public class MyProducer {
    public static void main(String[] args) throws InterruptedException {


        //1.创建kafka生产者的配置信息
        Properties props = new Properties();

        //ProducerConfig.BOOTSTRAP_SERVERS_CONFIG
        //ProducerConfig.ACKS_CONFIG

        //2.指定链接的kafka集群
        props.put("bootstrap.servers", "hadoop000:9092");

        //3. ack应答级别
        props.put("acks", "all");

        //4.重试次数
        props.put("retries", 1);

        //5.批次大小 16k
        props.put("batch.size", 16384);

        //6.等待时间
        props.put("linger.ms", 1);

        //7.RecordAccumulator 缓冲区大小 32M
        props.put("buffer.memory", 33554432);

        //8.序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //9.创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        //10.发送数据
        for (int i = 0; i < 1000; i++) {
            producer.send(new ProducerRecord<String, String>("first", "atguigu" + i));
            System.out.println("atguigu" + i);
        }
        //Thread.sleep(100);

        //11.关闭链接
        producer.close();


    }

}
