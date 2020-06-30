package flink.streaming.topN.HotGoodsTopN;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * create by liuzhiwei on 2020/6/14
 * 拿到窗口的开始时间和结束时间，拿出分组字段
 * 第一个：输入的数据类型（Long类型的次数），也就是 MyWindowAggFunction中聚合后的结果值
 * 第二个：输出的数据类型（ItemViewCount）
 * 第三个：分组的key(分组的字段)
 * 第四个：窗口对象（起始时间、结束时间）
 */
public class MyWindowFunction implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {
    @Override
    public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
        String itemId = tuple.getField(0);
        String type = tuple.getField(1);

        long windowStart = window.getStart();
        long windowEnd = window.getEnd();

        //窗口集合的结果
        Long aLong = input.iterator().next();

        //输出数据
        out.collect(ItemViewCount.of(itemId, type, windowStart, windowEnd, aLong));
    }
}
