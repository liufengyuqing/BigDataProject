package flink.streaming.topN.HotGoodsTopN;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * create by liuzhiwei on 2020/6/14
 * https://mp.weixin.qq.com/s/cxFOF2lB2oEZCcXui0gODw
 */
public class HotGoodsTopN {
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


        //窗口内的数据进行聚合，拿出窗口Star时间和窗口End时间
        //参数：输入的数据类， 输出的数据类，分组字段tuple， 窗口对象TimeWindow
        SingleOutputStreamOperator<ItemViewCount> result = window.apply(new WindowFunction<MyBehavior, ItemViewCount, Tuple, TimeWindow>() {
            @Override
            public void apply(Tuple tuple, TimeWindow window, Iterable<MyBehavior> input, Collector<ItemViewCount> out) throws Exception {
                //拿出分组的字段
                String itemId = tuple.getField(0);
                String type = tuple.getField(1);

                //拿出窗口的起始时间和结束时间
                long start = window.getStart();
                long end = window.getEnd();

                // 编写累加的逻辑
                int count = 0;
                for (MyBehavior myBehavior : input) {
                    count += 1;
                }
                //输出结果
                out.collect(ItemViewCount.of(itemId, type, start, end, count));
            }
        });



        //SingleOutputStreamOperator<Object> aggregate1 = window.aggregate(new MyWindowAggFunction(), new MyWindowFunction());

        result.print();

        env.execute("HotGoodsTopN");

    }
}
