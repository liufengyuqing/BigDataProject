package myproducer;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

/**
 * Hello world!
 */
public class LoanUserCreditsInfo {
    public static void main(String[] args) {
        Properties kafka = new Properties();
        kafka.setProperty("bootstrap.servers", "localhost:9092");
        kafka.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafka.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(kafka);
        String[] orderSeq = {"A", "B"};
        int[] traceId = {10, 15, 20, 25, 30, 35, 40};
        try {
            while (true) {
                Map<String, String> map = new HashMap<>();
                map.put("LOAN_ACCOUNT_ID", String.valueOf(new Random().nextInt(10)));
                map.put("CREDITS_STATUS", orderSeq[new Random().nextInt(orderSeq.length)]);
                map.put("TIME_CREATED", String.valueOf(System.currentTimeMillis()));
                map.put("TOTAL_CREDITS", String.valueOf(100 * Integer.valueOf(traceId[new Random().nextInt(traceId.length)])));
                Map<String, Object> r = new HashMap<>();
                r.put("table", "loan_user_credits_info");
                r.put("data", map);
                r.put("database", "loan_prod");
                r.put("isDdl", "false");
                String s = JSON.toJSONString(r);
                producer.send(new ProducerRecord<>("loan_prod", s));
                System.out.println(s);
                Thread.sleep(2000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
