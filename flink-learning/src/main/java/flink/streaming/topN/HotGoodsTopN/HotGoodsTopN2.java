package flink.streaming.topN.HotGoodsTopN;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * create by liuzhiwei on 2020/6/14
 * https://mp.weixin.qq.com/s/cxFOF2lB2oEZCcXui0gODw
 * <p>
 * 优化点：在窗口内增量聚合 (来一个加一个，内存中只保存一个数字而已)
 */
public class HotGoodsTopN2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 选择EventTime作为Flink的时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 设置checkPoint时间
        env.enableCheckpointing(60000);
        // 设置并行度
        env.setParallelism(1);

        DataStreamSource<String> lines = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<MyBehavior> process = lines.process(new ProcessFunction<String, MyBehavior>() {
            @Override
            public void processElement(String value, Context ctx, Collector<MyBehavior> out) throws Exception {
                try {
                    // FastJson 会自动把时间解析成long类型的TimeStamp
                    MyBehavior behavior = JSON.parseObject(value, MyBehavior.class);
                    out.collect(behavior);
                } catch (Exception e) {
                    e.printStackTrace();
                    //TODO 记录出现异常的数据
                }
            }
        });

        //提取EventTime,转换成Timestamp格式,生成WaterMark
        //设定延迟时间
        SingleOutputStreamOperator<MyBehavior> timestampsAndWatermarks = process.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<MyBehavior>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(MyBehavior element) {
                return element.timestamp;
            }
        });

        //按照指定事件分组
        //某个商品，在窗口时间内，被（点击、购买、添加购物车、收藏）了多少次
        KeyedStream<MyBehavior, Tuple> myBehaviorTupleKeyedStream = timestampsAndWatermarks.keyBy("itemId", "type");


        //把分好组的数据，划分窗口：假设窗口总长10分钟， 步长1分钟滑动一次
        WindowedStream<MyBehavior, Tuple, TimeWindow> window = myBehaviorTupleKeyedStream.window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(1)));


//        //窗口内的数据进行聚合，拿出窗口Star时间和窗口End时间
//        //参数：输入的数据类， 输出的数据类，分组字段tuple， 窗口对象TimeWindow
//        SingleOutputStreamOperator<ItemViewCount> result = window.apply(new WindowFunction<MyBehavior, ItemViewCount, Tuple, TimeWindow>() {
//            @Override
//            public void apply(Tuple tuple, TimeWindow window, Iterable<MyBehavior> input, Collector<ItemViewCount> out) throws Exception {
//                //拿出分组的字段
//                String itemId = tuple.getField(0);
//                String type = tuple.getField(1);
//
//                //拿出窗口的起始时间和结束时间
//                long start = window.getStart();
//                long end = window.getEnd();
//
//                // 编写累加的逻辑
//                int count = 0;
//                for (MyBehavior myBehavior : input) {
//                    count += 1;
//                }
//                //输出结果
//                out.collect(ItemViewCount.of(itemId, type, start, end, count));
//            }
//        });


        /**  使用这种aggregate聚合方法：
         *
         *    public <ACC, V, R> SingleOutputStreamOperator<R> aggregate(
         *       AggregateFunction<T, ACC, V> aggFunction,
         *       WindowFunction<V, R, K, W> windowFunction) {}
         */
        SingleOutputStreamOperator<ItemViewCount> windowAggregate = window.aggregate(new MyWindowAggFunction(), new MyWindowFunction());

        KeyedStream<ItemViewCount, Tuple> soredKeyed = windowAggregate.keyBy("type", "windowStart", "windowEnd");

        SingleOutputStreamOperator<List<ItemViewCount>> sored = soredKeyed.process(new KeyedProcessFunction<Tuple, ItemViewCount, List<ItemViewCount>>() {
            private transient ValueState<List<ItemViewCount>> valueState;

            // 要把这个时间段的所有的ItemViewCount作为中间结果聚合在一块，引入ValueState
            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<List<ItemViewCount>> VSDescriptor =
                        new ValueStateDescriptor<>("list-state",
                                TypeInformation.of(new TypeHint<List<ItemViewCount>>() {
                                })
                        );

                valueState = getRuntimeContext().getState(VSDescriptor);

            }

            //更新valueState 并注册定时器
            @Override
            public void processElement(ItemViewCount input, Context ctx, Collector<List<ItemViewCount>> out) throws Exception {
                List<ItemViewCount> buffer = valueState.value();
                if (buffer == null) {
                    buffer = new ArrayList<>();
                }
                buffer.add(input);
                valueState.update(buffer);
                //注册定时器,当为窗口最后的时间时，通过加1触发定时器
                ctx.timerService().registerEventTimeTimer(input.windowEnd + 1);

            }

            // 做排序操作
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<List<ItemViewCount>> out) throws Exception {

                //将ValueState中的数据取出来
                List<ItemViewCount> buffer = valueState.value();
                buffer.sort(new Comparator<ItemViewCount>() {
                    @Override
                    public int compare(ItemViewCount o1, ItemViewCount o2) {
                        //按照倒序，转成int类型
                        return -(int) (o1.viewCount - o2.viewCount);
                    }
                });
                valueState.update(null);
                out.collect(buffer);
            }
        });
        sored.print();
        env.execute("HotGoodsTopNAdv");
    }
}
