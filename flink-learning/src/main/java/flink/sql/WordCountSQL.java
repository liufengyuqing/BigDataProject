package flink.sql;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * create by liuzhiwei on 2020/4/3
 */
public class WordCountSQL {
    public static void main(String[] args) throws Exception {
        //创建上下文环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment fbTableEnv = BatchTableEnvironment.create(env);


        String words = "Hello Flink Hello Spark";
        String[] split = words.split("\\W+");

        List list = new ArrayList();
        for (String s : split) {
            WC wc = new WC(s, 1);
            list.add(wc);
        }

        DataSet<WC> input = env.fromCollection(list);

        //注册成Flink table
        fbTableEnv.registerDataSet("WordCount", input, "word, nums");
        Table table = fbTableEnv.sqlQuery("select word, sum(nums) as nums from WordCount group by word");

        DataSet<WC> wcDataSet = fbTableEnv.toDataSet(table, WC.class);

        wcDataSet.print();

    }

    /*public static class WC {
        public String word;
        public int frequency;

        public WC() {
        }

        public WC(String word, int frequency) {
            this.word = word;
            this.frequency = frequency;
        }

        @Override
        public String toString() {
            return "WC{" +
                    "word='" + word + '\'' +
                    ", frequency=" + frequency +
                    '}';
        }
    }*/
}
