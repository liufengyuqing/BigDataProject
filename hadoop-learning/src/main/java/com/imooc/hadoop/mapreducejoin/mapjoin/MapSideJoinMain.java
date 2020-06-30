package com.imooc.hadoop.mapreducejoin.mapjoin;

import com.imooc.hadoop.mapreducejoin.table.TableBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.util.HashMap;

/**
 * create by liuzhiwei on 2020/4/24
 * 在map端join
 * 使用场景：一张表十分小、一张表很大。
 */
public class MapSideJoinMain {
    private static final Logger logger = LoggerFactory.getLogger(MapSideJoinMain.class);


    private static class LeftOutJoinMapper extends Mapper<LongWritable, Text, Text, Text> {

        private HashMap<String, String> user_info = new HashMap();

        private Text k = new Text();
        private Text v = new Text();

        /**
         * 此方法在每个task开始之前执行，这里主要用作从DistributedCache
         * 中取到customer文件，并将里边记录取出放到内存中。
         *
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            //获得当前作业的DistributedCache相关文件 分布式缓存
            // 1 获取缓存的文件
            URI[] cacheFiles = context.getCacheFiles();
            String path = cacheFiles[0].getPath().toString();
            String userInfo;
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));
            while (null != (userInfo = br.readLine())) {
                // 2 切割
                String[] split = userInfo.split(",");
                System.out.println(split[1] + "," + split[2]);
                // 3 缓存数据到集合
                user_info.put(split[0], split[1] + "," + split[2]);
            }
            // 4 关流
            br.close();

        }

        /**
         * Map端的实现相当简单，直接判断order中的
         * userId是否存在我的map中就ok了，这样就可以实现Map Join了
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //排除空行
            if (value == null || value.toString().equals("")) {
                return;
            }

            String mapInputStr = value.toString();
            String[] mapInputSplit = mapInputStr.split(",");

            //过滤非法记录
            if (mapInputSplit.length != 4) {
                return;
            }

            String userInfo = (String) user_info.get(mapInputSplit[1]);
            if (userInfo != null) {
                k.set(mapInputSplit[0]);
                v.set(mapInputSplit[1] + "," + userInfo + "," + mapInputSplit[0] + "," + mapInputSplit[2] + "," + mapInputSplit[2]);
                context.write(k, v);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // 0 根据自己电脑路径重新配置
        args = new String[]{"hadoop-learning/src/input", "hadoop-learning/src/output"};

        // 1 获取配置信息，或者job对象实例
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //准备清理已存在的输出目录
        Path outputPath = new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
            System.out.println("output file exists, but is has deleted");
        }

        // 2 指定本程序的jar包所在的本地路径
        job.setJarByClass(MapSideJoinMain.class);

        // 3 指定本业务job要使用的Mapper/Reducer业务类
        job.setMapperClass(LeftOutJoinMapper.class);

        // 4 指定Mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);

        // 5 指定最终输出的数据的kv类型
        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);

        // 6 指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 6 加载缓存数据
        job.addCacheFile(new URI("hadoop-learning/src/cache/customers.txt"));

        // 7 Map端Join的逻辑不需要Reduce阶段，设置reduceTask数量为0
        job.setNumReduceTasks(0);

        // 8 将job中配置的相关参数，以及job所用的java类所在的jar包， 提交给yarn去运行
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
