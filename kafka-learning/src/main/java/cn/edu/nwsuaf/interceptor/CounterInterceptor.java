package cn.edu.nwsuaf.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @ClassName: CounterInterceptor
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/2/20 1:47 下午
 */

public class CounterInterceptor implements ProducerInterceptor<String, String> {
    int success;
    int error;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (metadata != null) {
            success++;
        } else {
            error++;
        }
    }

    @Override
    public void close() {
        System.out.println("success: " + success);
        System.out.println("error: " + error);

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
