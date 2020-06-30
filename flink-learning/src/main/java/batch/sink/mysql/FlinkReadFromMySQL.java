package batch.sink.mysql;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * create by liuzhiwei on 2020/4/1
 */
public class FlinkReadFromMySQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Student> source = env.addSource(new SourceFromMySQL());
        source.print("来自mysql的数据：");
        env.execute("FlinkReadFromMySQL");
    }
}
