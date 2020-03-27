package redis;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * create by liuzhiwei on 2020/3/23
 */
public class TestRedis {
    Jedis jedis = null;

    @Before
    public void before() {
        jedis = new Jedis("localhost");
    }

    @After
    public void after() {
        if (jedis != null) {
            jedis.close();
        }

    }


    public static void main(String[] args) {
        final Jedis jedis = new Jedis("localhost");
        System.out.println("链接本地 Redis 服务");
        System.out.println("服务正在运营" + jedis.ping());
        // 设置 redis 字符串数据
        jedis.set("age", "18");
        // 获取存储的数据并输出
        System.out.println("redis存储的字符串是: " + jedis.get("age"));

        jedis.del("kecheng");
        // 存储数据到列表中
        jedis.lpush("kecheng", "java");
        jedis.lpush("kecheng", "php");
        jedis.lpush("kecheng", "Mysql");
        jedis.rpush("kecheng", "hive");

        // 获取存储的数据并输出
        List<String> list = jedis.lrange("kecheng", 0, 5);
        for (int i = 0; i < list.size(); i++) {
            System.out.println("redis list里面存储的值是:" + list.get(i));
        }

        //开始前，先移除所有的内容
        jedis.del("kecheng");
        System.out.println(jedis.lrange("kecheng", 0, -1));
        jedis.lpush("kecheng", "java");
        jedis.lpush("kecheng", "php");
        jedis.lpush("kecheng", "Mysql");
        jedis.rpush("kecheng", "hive");

        //再取出所有数据jedis.lrange是按范围取出
        System.out.println(jedis.lrange("kecheng", 0, -1));

        /**
         * map
         */

        final HashMap<String, String> hashMap = new HashMap<>();
        hashMap.put("name", "liuzhiwei");
        hashMap.put("age", "18");
        hashMap.put("sex", "male");

        jedis.hmset("user", hashMap);
        //第一个参数是存入redis中map对象的key，后面跟的是放入map中的对象的key，后面的key可以跟多个，是可变参数
        final List<String> hmget = jedis.hmget("user", "name", "age", "sex");
        System.out.println(hmget);

        //删除map中的某个键值
        jedis.hdel("user", "age");
        System.out.println(jedis.hmget("user", "age"));//因为删除了，所以返回的是null
        System.out.println(jedis.hlen("user")); //返回key为user的键中存放的值的个数2
        System.out.println(jedis.exists("user"));//是否存在key为user的记录 返回true
        System.out.println(jedis.hkeys("user"));//返回map对象中的所有keys
        System.out.println(jedis.hvals("user"));//返回map对象中的所有value

        final Iterator<String> it = jedis.hkeys("user").iterator();
        while (it.hasNext()) {
            final String key = it.next();
            System.out.println(key + ":" + jedis.hmget("user", key));
        }
    }

    /**
     * Redis Java Set 实例
     */
    @Test
    public void testSet() {
        jedis.sadd("users", "liuling");
        jedis.sadd("users", "xinxin");
        jedis.sadd("users", "ling");
        jedis.sadd("users", "zhangxinxin");
        jedis.sadd("users", "who");


        jedis.srem("users", "who");
        System.out.println(jedis.smembers("users"));//获取所有加入的value
        System.out.println(jedis.sismember("users", "who"));//判断 who 是否是user集合的元素
        System.out.println(jedis.srandmember("users"));
        System.out.println(jedis.scard("users"));//返回集合的元素个数
    }

    /**
     * Redis Java Sort实例
     */
    @Test
    public void testSort() {
        jedis.del("a");
        jedis.rpush("a", "1");
        jedis.rpush("a", "6");
        jedis.rpush("a", "3");
        jedis.rpush("a", "9");
        jedis.rpush("a", "4");
        System.out.println(jedis.lrange("a", 0, -1));
        System.out.println(jedis.sort("a"));
        System.out.println(jedis.lrange("a", 0, -1));
    }


    @Test
    public void keyOperate() {
        System.out.println("======================key==========================");
        //清空数据
        System.out.println("清空库中所有数据" + jedis.flushDB());
        System.out.println("清空所有数据" + jedis.flushAll());

        System.out.println("判断key999键是否存在：" + jedis.exists("key999"));

        // 输出系统中所有的key
        final Set<String> keys = jedis.keys("*");
        final Iterator<String> iterator = keys.iterator();
        while (iterator.hasNext()) {
            String key = iterator.next();
            System.out.println(key);
        }

        System.out.println("查看key001的剩余生存时间" + jedis.ttl("key01"));

        System.out.println("查看key所储存的值的类型：" + jedis.type("key001"));
        jedis.rename("key6", "key0");
        System.out.println("将当前db的key移动到给定的db当中" + jedis.move("foo", 1));


    }


}
