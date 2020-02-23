package com.imooc.flink.course04;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import scala.Int;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName: DataSetTransformationApp
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/2/8 4:22 下午
 */

public class JavaDataSetTransformationApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //mapFunction(env);

        //filterFunction(env);

        //mapPartitionFunction(env);

        //irstFunction(env);

        //flatMapfunction(env);

        //distinctionFunction(env);

        //joinFunction(env);

        crossFunction(env);

    }

    public static void mapFunction(ExecutionEnvironment env) throws Exception {
        List<Integer> list = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            list.add(i);
        }

        DataSource<Integer> data = env.fromCollection(list);

        data.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer input) throws Exception {
                return input + 1;
            }
        }).print();

    }

    public static void filterFunction(ExecutionEnvironment env) throws Exception {
        List<Integer> list = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            list.add(i);
        }

        DataSource<Integer> data = env.fromCollection(list);

        data.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer input) throws Exception {
                return input + 1;
            }
        }).filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer integer) throws Exception {
                return integer > 5;
            }
        }).print();
    }

    public static void mapPartitionFunction(ExecutionEnvironment env) throws Exception {
        List<String> students = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            students.add("student" + i);
        }

        DataSet<String> data = env.fromCollection(students).setParallelism(4);
//        data.map(new MapFunction<String, String>() {
//            @Override
//            public String map(String s) throws Exception {
//                String connection = DBUtils.getConnection();
//                System.out.println("connection" + connection);
//                DBUtils.returnConnection(connection);
//                return s;
//            }
//        }).print();

        data.mapPartition(new MapPartitionFunction<String, String>() {
            @Override
            public void mapPartition(Iterable<String> iterable, Collector<String> collector) throws Exception {
                String connnection = DBUtils.getConnection();
                System.out.println("connection:" + connnection);
                DBUtils.returnConnection(connnection);

            }
        }).print();
    }

    public static void firstFunction(ExecutionEnvironment env) throws Exception {
        List<Tuple2<Integer, String>> info = new ArrayList();
        info.add(new Tuple2<>(1, "hadoop"));
        info.add(new Tuple2<>(1, "spark"));
        info.add(new Tuple2<>(1, "flink"));
        info.add(new Tuple2<>(2, "java"));
        info.add(new Tuple2<>(2, "spring"));
        info.add(new Tuple2<>(3, "linux"));
        info.add(new Tuple2<>(4, "js"));

        DataSource<Tuple2<Integer, String>> data = env.fromCollection(info);

        data.groupBy(0).sortGroup(1, Order.ASCENDING).first(2).print();
        System.out.println("-----------");
        data.groupBy(0).sortGroup(1, Order.DESCENDING).first(2).print();
    }

    public static void flatMapfunction(ExecutionEnvironment env) throws Exception {
        List<String> info = new ArrayList();
        info.add("hadoop,spark");
        info.add("spark,flink");

        DataSource<String> data = env.fromCollection(info);
        data.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] tokens = s.split(",");
                for (String token : tokens) {
                    collector.collect(token);
                }
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        }).groupBy(0).sum(1).print();

    }

    public static void distinctionFunction(ExecutionEnvironment env) throws Exception {
        List<String> info = new ArrayList();
        info.add("hadoop,spark");
        info.add("spark,flink");

        DataSource<String> data = env.fromCollection(info);
        data.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] tokens = s.split(",");
                for (String token : tokens) {
                    collector.collect(token);
                }
            }
        }).distinct().print();
    }

    public static void joinFunction(ExecutionEnvironment env) throws Exception {
        List<Tuple2<Integer, String>> info1 = new ArrayList<>();
        info1.add(new Tuple2<>(1, "pk"));
        info1.add(new Tuple2<>(2, "j"));
        info1.add(new Tuple2<>(3, "a"));
        info1.add(new Tuple2<>(4, "b"));

        List<Tuple2<Integer, String>> info2 = new ArrayList<>();
        info2.add(new Tuple2<>(1, "北京"));
        info2.add(new Tuple2<>(2, "成都"));
        info2.add(new Tuple2<>(3, "上海"));
        info2.add(new Tuple2<>(5, "杭州"));

        DataSource<Tuple2<Integer, String>> data1 = env.fromCollection(info1);
        DataSource<Tuple2<Integer, String>> data2 = env.fromCollection(info2);

        data1.join(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                return new Tuple3<Integer, String, String>(first.f0, first.f1, second.f1);
            }
        }).print();


        System.out.println(" -------------- ");
        data1.leftOuterJoin(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                if (second == null) {
                    return new Tuple3<Integer, String, String>(first.f0, first.f1, "-");
                } else {
                    return new Tuple3<Integer, String, String>(first.f0, first.f1, second.f1);
                }

            }
        }).print();

        System.out.println(" -------------- ");
        data1.rightOuterJoin(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                if (first == null) {
                    return new Tuple3<Integer, String, String>(second.f0, second.f1, "-");
                } else {
                    return new Tuple3<Integer, String, String>(first.f0, first.f1, second.f1);
                }
            }
        }).print();

        System.out.println(" -------------- ");
        data1.fullOuterJoin(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                if (first == null) {
                    return new Tuple3<Integer, String, String>(second.f0, second.f1, "-");
                } else if (second == null) {
                    return new Tuple3<Integer, String, String>(first.f0, first.f1, "-");
                } else {
                    return new Tuple3<Integer, String, String>(first.f0, first.f1, second.f1);
                }
            }
        }).print();

    }

    public static void crossFunction(ExecutionEnvironment env) throws Exception {
        List<String> info1 = new ArrayList<>();
        info1.add("曼联");
        info1.add("曼城");

        List<String> info2 = new ArrayList<>();
        info2.add("3");
        info2.add("2");
        info2.add("1");

        DataSource<String> data1 = env.fromCollection(info1);
        DataSource<String> data2 = env.fromCollection(info2);

        data1.cross(data2).print();

    }

}
