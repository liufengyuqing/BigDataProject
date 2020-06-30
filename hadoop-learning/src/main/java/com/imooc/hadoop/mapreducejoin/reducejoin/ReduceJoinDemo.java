package com.imooc.hadoop.mapreducejoin.reducejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * create by liuzhiwei on 2020/4/24
 */
public class ReduceJoinDemo {

    private static class MyMapper extends Mapper<LongWritable, Text, Text, JoinWritable> {

        private Text k = new Text();
        private JoinWritable bean = new JoinWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String lineValue = value.toString();

            //分割CSV文件
            String[] values = lineValue.split(",");

            //过滤脏数据
            int length = values.length;
            if (length != 3 && length != 4) {
                return;
            }
            if (length == 3) {
                String name = values[1];
                String phone = values[2];
                k.set(values[0]);
                bean.setTag("customer");
                bean.setData(name + "," + phone);
            } else if (length == 4) {
                String orderId = values[0];
                String price = values[2];
                String date = values[3];
                k.set(values[1]);
                bean.setTag("order");
                bean.setData(orderId + "," + price + "," + date);
            }
            context.write(k, bean);
        }
    }


    private static class MyReducer extends Reducer<Text, JoinWritable, Text, NullWritable> {

        private Text reduceOutPutKey = new Text();

        @Override
        protected void reduce(Text key, Iterable<JoinWritable> values, Context context) throws IOException, InterruptedException {
            // 用户信息
            String user = null;
            // 订单信息，一个用户可能对应多个订单信息，所以需要用容器
            List<String> list = new ArrayList<>();

            for (JoinWritable value : values) {
                if ("customer".equals(value.getTag())) {
                    user = value.getData();
                } else {
                    list.add(value.getData());
                }
            }
            // 以 "userId,用户信息，订单信息"  的形式 设置key
            for (String s : list) {
                reduceOutPutKey.set(key + "," + user + "," + s);
                context.write(reduceOutPutKey, NullWritable.get());
            }
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"hadoop-learning/src/input", "hadoop-learning/src/output"};

        //创建Configuration
        Configuration configuration = new Configuration();

        //准备清理已存在的输出目录
        Path outputPath = new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
            System.out.println("output file exists, but is has deleted");
        }

        //创建Job
        Job job = Job.getInstance(configuration, "ReduceJoinDemo");


        //设置job的处理类
        job.setJarByClass(ReduceJoinDemo.class);

        //设置作业 处理的输入路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        //设置map相关参数
        job.setMapperClass(ReduceJoinDemo.MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(JoinWritable.class);

        //设置reduce相关参数
        job.setReducerClass(ReduceJoinDemo.MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //设置作业处理的输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }


}
