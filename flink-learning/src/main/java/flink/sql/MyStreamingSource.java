package flink.sql;

import lombok.Data;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.Random;

/**
 * create by liuzhiwei on 2020/5/20
 */
public class MyStreamingSource implements SourceFunction<Item> {
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

    //随机产生一条商品数据
    private Item generateItem() {
        int i = new Random().nextInt(100);
        ArrayList<String> list = new ArrayList();
        list.add("HAT");
        list.add("TIE");
        list.add("SHOE");

        Item item = new Item();
        item.setName(list.get(new Random().nextInt(3)));
        item.setId(i);
        return item;
    }
}

class StreamingDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Item> items = env.addSource(new MyStreamingSource()).setParallelism(1);

        SingleOutputStreamOperator<Item> item = items.map((value -> value)).returns(TypeInformation.of(Item.class));

        //map 使用map转化为name
        SingleOutputStreamOperator<Object> mapItems = item.map(new MapFunction<Item, Object>() {
            @Override
            public Object map(Item value) throws Exception {
                return value.getName();
            }
        });

        //lambda
        //SingleOutputStreamOperator<String> map = items.map(ite -> ite.getName());


        item.print().setParallelism(1);

        env.execute("user defined streaming source");

    }
}
