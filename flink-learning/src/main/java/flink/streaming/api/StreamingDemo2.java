package flink.streaming.api;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.expressions.In;

import java.util.ArrayList;
import java.util.List;

/**
 * create by liuzhiwei on 2020/5/20
 * <p>
 * max 和 maxBy的例子
 * min 和 minBy 都会返回整个元素，只是 min 会根据用户指定的字段取最小值，
 * 并且把这个值保存在对应的位置，而对于其他的字段，并不能保证其数值正确。max 和 maxBy 同理。
 */
public class StreamingDemo2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        List<Tuple3<Integer, Integer, Integer>> data = new ArrayList<>();
        data.add(new Tuple3<>(0, 1, 1));
        data.add(new Tuple3<>(0, 1, 3));
        data.add(new Tuple3<>(0, 2, 2));
        data.add(new Tuple3<>(0, 1, 0));

        data.add(new Tuple3<>(1, 2, 5));
        data.add(new Tuple3<>(1, 2, 11));
        data.add(new Tuple3<>(1, 2, 13));
        data.add(new Tuple3<>(1, 2, 9));


        DataStreamSource<Tuple3<Integer, Integer, Integer>> items = env.fromCollection(data);

        items.keyBy(0).maxBy(2).print().setParallelism(1);

        env.execute("StreamingDemo2");

    }
}
