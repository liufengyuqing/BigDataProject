package cn.edu.nwsuaf.publish.sunscribe;

import redis.clients.jedis.Jedis;

/**
 * @ClassName: Publisher
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/3/26 3:37 下午
 */

public class Publisher {
    public static void main(String[] args) {
        Jedis jedis = new Jedis("localhost", 6379);
        System.out.println("我是发布者。。。");

        jedis.publish("channel2", "hello world");

        System.out.println("消息已发送完毕");
    }
}
