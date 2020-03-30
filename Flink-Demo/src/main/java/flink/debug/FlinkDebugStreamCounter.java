package flink.debug;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.stream.Stream;

/**
 * create by liuzhiwei on 2020/3/27
 */
public class FlinkDebugStreamCounter {
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
                .flatMap(new RichFlatMapFunction<String, KeyCount>() {
                    private LongCounter lineCounter = new LongCounter(0);
                    private LongCounter wordCounter = new LongCounter(0);

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        getRuntimeContext().addAccumulator("line-number", lineCounter);
                        getRuntimeContext().addAccumulator("word-number", wordCounter);
                    }

                    @Override
                    public void flatMap(String line, Collector<KeyCount> out) throws Exception {
                        //Java8 流
                        Stream.of(line.split("\\s+"))
                                .forEach(word -> {
                                    out.collect(new KeyCount(word, 1));
                                    wordCounter.add(1);
                                });
                        lineCounter.add(1);
                        //lambda表达式 不能推断出类型
                    }
                }).returns(Types.POJO(KeyCount.class))
                .keyBy(new KeySelector<KeyCount, String>() {
                    @Override
                    public String getKey(KeyCount value) throws Exception {
                        return value.getWord();
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
        private String word;
        private int count;
    }
}
