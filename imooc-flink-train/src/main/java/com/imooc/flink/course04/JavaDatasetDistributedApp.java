package com.imooc.flink.course04;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName: JavaDatasetDistributedApp
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/2/8 9:21 下午
 */

public class JavaDatasetDistributedApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String filePath = "file:///Users/liuzhiwei/data/hello.txt";

        //step1：注册一个本地/HDFS文件
        env.registerCachedFile(filePath, "pk-scala-dc");

        DataSource<String> data = env.fromElements("hadoop", "spark", "flink", "pyspark", "storm");

        data.map(new RichMapFunction<String, String>() {
            List<String> list = new ArrayList<>();

            // step2: 在open方法中获取分布式缓存的内容即可
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                File file = getRuntimeContext().getDistributedCache().getFile("pk-scala-dc");
                List<String> lines = FileUtils.readLines(file);

                for (String line : lines) {
                    list.add(line);
                    System.out.println("line: " + line);
                }
            }

            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).print();


    }
}
