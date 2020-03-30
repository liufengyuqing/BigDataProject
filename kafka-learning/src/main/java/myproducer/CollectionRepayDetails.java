package myproducer;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

/**
 * create by liuzhiwei on 2020/3/27
 */
public class CollectionRepayDetails {
    public static void main(String[] args) {
        {
            Properties kafka = new Properties();
            kafka.setProperty("bootstrap.servers", "localhost:9092");
            kafka.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            kafka.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            KafkaProducer<String, String> producer = new KafkaProducer<>(kafka);
            String[] collectorId = {"1", "2", "3", "4", "5"};
//        Class.forName("com.mysql.jdbc.Driver");
//        Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/loan_prod?characterEncoding=UTF-8&useSSL=false", "root", "root");
//
//        String sql = "replace into loan_prod.loan_register_channel_info (id, act_id, loan_account_id) values(?,?,?)";
//        PreparedStatement ps = conn.prepareStatement(sql);
            try {
                while (true) {
                    Thread.sleep(1000);
                    Map<String, String> map = new HashMap<>();
                    int id = new Random().nextInt(10);
                    map.put("id", String.valueOf(id));
                    String collector_id = collectorId[new Random().nextInt(collectorId.length)];
                    map.put("collector_id", collector_id);
                    map.put("time_dialing", String.valueOf(System.currentTimeMillis()));
//                ps.setInt(1, id);w
//                ps.setString(2, act);
//                ps.setInt(3, id);
//                ps.execute();
                    Map<String, Object> r = new HashMap<>();
                    r.put("table", "loan_register_channel_info");
                    r.put("data", map);
                    r.put("database", "loan_prod");
                    r.put("isDdl", "false");
                    String s = JSON.toJSONString(r);
                    producer.send(new ProducerRecord<>("loan_prod", s));
                    System.out.println(s);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
