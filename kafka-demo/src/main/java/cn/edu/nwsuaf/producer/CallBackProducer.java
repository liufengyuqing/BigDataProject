package cn.edu.nwsuaf.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @ClassName: CallBackProducer
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/2/20 11:35 上午
 */

public class CallBackProducer {
    public static void main(String[] args) {

        //1.创建配置信息
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop000:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        //2.创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        //3. 发送数据
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>("aaa", 0, "atguigu", "atguigu" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        //offset 从0开始，每个分区有序，
                        System.out.println(metadata.partition() + "--" + metadata.offset());
                    } else {
                        exception.printStackTrace();
                    }
                }
            });

        }
        //4.关闭数据
        producer.close();
    }
}
