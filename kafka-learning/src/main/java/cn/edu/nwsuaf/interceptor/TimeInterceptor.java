package cn.edu.nwsuaf.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @ClassName: TimeInterceptor
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/2/20 1:42 下午
 */

public class TimeInterceptor implements ProducerInterceptor<String, String> {
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {

        //取出数据
        String value = record.value();

        //2 创建一个新的ProducerRecord对象 并返回
        return new ProducerRecord<>(record.topic(), System.currentTimeMillis() + "," + value);
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {


    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
