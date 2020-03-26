package cn.edu.nwsuaf.publish.sunscribe;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

/**
 * @ClassName: Subscriber
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/3/26 3:40 下午
 */

public class Subscriber extends JedisPubSub {
    @Override
    public void onMessage(String channel, String message) {
        super.onMessage(channel, message);
        System.out.println("订阅渠道: " + channel + " 收到的消息: " + message);
    }

    public static void main(String[] args) {
        Jedis jedis = new Jedis("localhost", 6379);
        Subscriber subscriber = new Subscriber();

        //从redis订阅消息
        System.out.println("我是订阅者。。。");
        jedis.subscribe(subscriber, "channel2");
    }
}
