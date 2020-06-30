package com.imooc.hadoop.flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * create by liuzhiwei on 2020/4/19
 */
public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    FlowBean v = new FlowBean();
    Text k = new Text();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //super.map(key, value, context);
        //7 	13560436666	120.196.100.99		1116		 954			200
        //id	手机号码		网络ip			 上行流量          下行流量     网络状态码

        //13560436666 		1116		      954 			2070
        //手机号码		    上行流量        下行流量		总流量

        // 1 获取一行
        String line = value.toString();

        // 2 切割字段
        String[] split = line.split("\t");

        // 3 封装对象
        // 取出手机号码
        String phoneNum = split[1];

        // 取出上行流量和下行流量
        long upFlow = Long.parseLong(split[split.length - 3]);
        long downFlow = Long.parseLong(split[split.length - 2]);

        k.set(phoneNum);
        v.setUpFlow(upFlow);
        v.setDownFlow(downFlow);

        // 4 写出
        context.write(k, v);
    }
}

