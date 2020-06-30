package flink.streaming.topN.HotGoodsTopN;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * create by liuzhiwei on 2020/6/14
 * 拿到聚合字段（MyBehavior中counts）
 * 第一个输入的类型
 * 第二个计数/累加器的类型
 * 第三个输出的数据类型
 */
public class MyWindowAggFunction implements AggregateFunction<MyBehavior, Long, Long> {
    //初始化一个计数器
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    //每输入一条数据就调用一次add方法
    @Override
    public Long add(MyBehavior value, Long accumulator) {
        return accumulator + value.counts;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    //只针对SessionWindow有效，对应滚动窗口、滑动窗口不会调用此方法
    @Override
    public Long merge(Long a, Long b) {
        return null;
    }
}
