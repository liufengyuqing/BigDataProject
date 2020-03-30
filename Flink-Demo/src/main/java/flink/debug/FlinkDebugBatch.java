package flink.debug;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.stream.Stream;

/**
 * create by liuzhiwei on 2020/3/27
 */
public class FlinkDebugBatch {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration() {
            {
                setInteger("rest.port", 9191);
                setBoolean("local.start-webserver", true);
            }
        };

        //创建带有本地webUI的调试环境
        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        //可变长参数
        env.fromElements(
                "appache flink",
                "flink graph",
                "flink stream",
                "flink batch",
                "batch sql"
        ).flatMap((String line, Collector<Tuple2<String, Integer>> out) ->
                //Java8 流
                Stream.of(line.split("\\s+")).forEach(word -> out.collect(Tuple2.of(word, 1))))
                //lambda表达式 不能推断出类型
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .groupBy(0)
                .sum(1)
                .print();

                        /*new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                        Stream.of(line.split("\\s+")).forEach(word -> out.collect(Tuple2.of(word,1)));
                    }}).*/


        //env.execute("Flink Batch Java API Skeleton");
        //防止程序不退出
        System.in.read();


    }
}
