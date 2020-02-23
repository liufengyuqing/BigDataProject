package com.imooc.flink.course04;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName: JavaDataSetDataSourceApp
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/2/8 9:33 上午
 */

public class JavaDataSetDataSourceApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //fromCollection(env);

        //textFile(env);

        readRecursiveFiles(env);
    }

    public static void fromCollection(ExecutionEnvironment env) throws Exception {
        List<Integer> list = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            list.add(i);
        }
        env.fromCollection(list).print();
    }

    public static void textFile(ExecutionEnvironment env) throws Exception {

        String filePath = "file:///Users/liuzhiwei/data/flink/input/hello.txt";
        env.readTextFile(filePath).print();

        System.out.println("~~~~~~华丽的分割线~~~~~~~~~");

        filePath = "file:///Users/liuzhiwei/data/flink/input";
        env.readTextFile(filePath).print();

    }

    public static void readRecursiveFiles(ExecutionEnvironment env) throws Exception {
        String filePath = "file:///Users/liuzhiwei/data/flink/input";

        Configuration conf = new Configuration();
        // create a configuration object
        Configuration parameters = new Configuration();

        // set the recursive enumeration parameter
        parameters.setBoolean("recursive.file.enumeration", true);

        // pass the configuration to the data source
        DataSet<String> logs = env.readTextFile(filePath)
                .withParameters(parameters);

        logs.print();

    }

}
