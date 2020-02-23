package com.imooc.flink.course04;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import scala.Int;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName: JavaDataSetSinkApp
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/2/8 6:32 下午
 */

public class JavaDataSetSinkApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<Integer> info = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            info.add(i);
        }

        DataSet<Integer> data = env.fromCollection(info);

        String filePath = "file:///Users/liuzhiwei/data/flink/input/sink-out-java";

        data.writeAsText(filePath, FileSystem.WriteMode.OVERWRITE);
        env.execute("JavaDataSetSinkApp");

    }
}
