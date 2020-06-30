package flink.streaming.topN;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * create by liuzhiwei on 2020/4/27
 */
public class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
        String[] split = value.toLowerCase().split("\\W+");

       /* for (String token : split) {
            out.collect(new Tuple2<>(token, 1));
        }*/

        //（书1,1） (书2，1) （书3,1）
        out.collect(new Tuple2<>(value, 1));
    }
}
