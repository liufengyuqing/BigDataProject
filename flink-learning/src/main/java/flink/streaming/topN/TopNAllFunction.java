package flink.streaming.topN;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

/**
 * create by liuzhiwei on 2020/4/27
 */


public class TopNAllFunction extends ProcessAllWindowFunction<Tuple2<String, Integer>, String, TimeWindow> {

    private int topSize = 3;

    public TopNAllFunction(int topSize) {
        this.topSize = topSize;
    }

    @Override
    public void process(Context context, Iterable<Tuple2<String, Integer>> elements, Collector<String> out) throws Exception {

        //treemap按照key降序排列，相同count值不覆盖
        TreeMap<Integer, Tuple2<String, Integer>> treeMap = new TreeMap<>(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o1 - o2;
            }
        });

        for (Tuple2<String, Integer> element : elements) {
            treeMap.put(element.f1, element);
            //只保留前面TopN个元素
            if (treeMap.size() > topSize) {
                treeMap.pollLastEntry();
            }
        }

        out.collect("=================\n热销图书列表:\n" + new Timestamp(System.currentTimeMillis()) + " " + treeMap.toString() + "\n=================\n");
/*
        for (Map.Entry<Integer, Tuple2<String, Integer>> entry : treeMap.entrySet()) {

        }*/

    }
}
