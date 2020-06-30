package flink.streaming.api;

import lombok.Data;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.expressions.E;

import java.util.Random;

/**
 * create by liuzhiwei on 2020/5/20
 */
public class MyStreamingSource implements SourceFunction<MyStreamingSource.Item> {
    private boolean isRunning = true;


    /**
     * 重写run方法产生一个源源不断的数据发送源
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<Item> ctx) throws Exception {
        while (isRunning) {
            Item item = generateItem();
            ctx.collect(item);
            //每秒产生一条数据
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    private Item generateItem() {
        int i = new Random().nextInt(100);
        Item item = new Item();
        item.setName("name" + i);
        item.setAge(i);
        return item;
    }


    @Data
    class Item {
        private String name;
        private Integer age;

    }
}

class StreamingDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<MyStreamingSource.Item> items = env.addSource(new MyStreamingSource()).setParallelism(1);

        SingleOutputStreamOperator<MyStreamingSource.Item> item = items.map((value -> value)).returns(TypeInformation.of(MyStreamingSource.Item.class));

        //map 使用map转化为name
        SingleOutputStreamOperator<Object> mapItems = item.map(new MapFunction<MyStreamingSource.Item, Object>() {
            @Override
            public Object map(MyStreamingSource.Item value) throws Exception {
                return value.getName();
            }
        });

        //lambda
        //SingleOutputStreamOperator<String> map = items.map(ite -> ite.getName());


        item.print().setParallelism(1);

        env.execute("user defined streaming source");

    }
}
