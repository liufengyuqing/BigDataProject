package cn.edu.nwsuaf.pipeline;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

/**
 * @ClassName: Pipeline
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/3/26 4:00 下午
 */

public class PipelineDemo {
    public static void main(String[] args) {
        Jedis jedis = new Jedis("localhost", 6379);

        Pipeline pipeline = jedis.pipelined();

        //通过pipline的方式
        long pipelineBegin = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
            pipeline.set("pipeline-test" + i, i + "");
        }
        //获取所有的response
        pipeline.sync();
        long pipelineEnd = System.currentTimeMillis();

        System.out.println("the pipeline is: " + (pipelineEnd - pipelineBegin));

        //通过jedis的方式
        long start = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
            jedis.set("jedis-test" + i, i + "");
        }

        long end = System.currentTimeMillis();

        System.out.println("the jedis is: " + (end - start));


    }
}
