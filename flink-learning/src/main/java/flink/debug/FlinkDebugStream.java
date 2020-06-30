package flink.debug;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.stream.Stream;

/**
 * create by liuzhiwei on 2020/3/27
 */
public class FlinkDebugStream {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration() {
            {
                setInteger("rest.port", 9191);
                setBoolean("local.start-webserver", true);
            }
        };

        //创建带有本地webUI的调试环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(4).setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        env.socketTextStream("localhost", 9999)
                .flatMap((String line, Collector<KeyCount> out) ->
                        //Java8 流
                        Stream.of(line.split("\\s+")).forEach(word -> out.collect(new KeyCount(word, 1))))
                //lambda表达式 不能推断出类型
                .returns(Types.POJO(KeyCount.class))
                .keyBy(new KeySelector<KeyCount, String>() {
                    @Override
                    public String getKey(KeyCount value) throws Exception {
                        return value.getKey();
                    }
                }).timeWindow(Time.seconds(10))
                .sum("count")
                .print();


        env.execute("Flink Streaming Java API Skeleton");
        //防止程序不退出
        //System.in.read();


    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class KeyCount {
        private String key;
        private int count;
    }
}
