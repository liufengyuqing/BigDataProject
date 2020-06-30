package flink.sql;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * create by liuzhiwei on 2020/4/3
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> source = env.fromElements("Hello", "Flink", "Hello", "Spark");

        AggregateOperator<Tuple2<String, Integer>> wordCount = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {

                String[] strings = value.split("\\W+");

                for (String string : strings) {
                    if (string.length() > 0) {
                        out.collect(new Tuple2<>(string, 1));
                    }
                }
            }
        }).groupBy(0)
                .sum(1);

        wordCount.print();
    }
}
