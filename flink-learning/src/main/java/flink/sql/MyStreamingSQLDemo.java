package flink.sql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;


import java.util.ArrayList;
import java.util.List;

/**
 * create by liuzhiwei on 2020/5/20
 */
public class MyStreamingSQLDemo {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        SingleOutputStreamOperator<Item> source = bsEnv.addSource(new MyStreamingSource()).map(new MapFunction<Item, Item>() {
            @Override
            public Item map(Item value) throws Exception {
                return value;
            }
        });

        SplitStream<Item> split = source.split(new OutputSelector<Item>() {
            @Override
            public Iterable<String> select(Item value) {
                List<String> output = new ArrayList<>();
                if (value.getId() % 2 == 0) {
                    output.add("even");
                } else {
                    output.add("odd");
                }
                return output;
            }
        });

        DataStream<Item> evenSelect = split.select("even");
        DataStream<Item> oddSelect = split.select("odd");

        bsTableEnv.registerDataStream("evenTable", evenSelect, "name,id");
        bsTableEnv.registerDataStream("oddTable", oddSelect, "name,id");

        Table table = bsTableEnv.sqlQuery("select a.id,a.name,b.id,b.name from evenTable as a join oddTable as b on a.name = b.name");

        table.printSchema();

        bsTableEnv.toRetractStream(table, TypeInformation.of(new TypeHint<Tuple4<Integer, String, Integer, String>>() {})).print();


        bsEnv.execute("streaming sql job");

    }
}
