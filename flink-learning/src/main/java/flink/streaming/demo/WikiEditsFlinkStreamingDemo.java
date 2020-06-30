package flink.streaming.demo;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;


/**
 * create by liuzhiwei on 2020/6/2
 * <p>
 * 使用flink分析wiki log
 */
public class WikiEditsFlinkStreamingDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取wikipedia 日志的source
        DataStreamSource<WikipediaEditEvent> source = env.addSource(new WikipediaEditsSource());

        //source.print("wiki日志形式：");
//        wiki日志形式：:4> WikipediaEditEvent{timestamp=1591079630720, channel='#en.wikipedia', title='Piano Concerto (Paderewski)', diffUrl='https://en.wikipedia.org/w/index.php?diff=960305814&oldid=960305775', user='Benedyktyn1', byteDiff=-6, summary='', flags=1}
//        wiki日志形式：:1> WikipediaEditEvent{timestamp=1591079632661, channel='#en.wikipedia', title='Baji Rao I', diffUrl='https://en.wikipedia.org/w/index.php?diff=960305815&oldid=960117353', user='Mahusha', byteDiff=11, summary='', flags=0}
//        wiki日志形式：:2> WikipediaEditEvent{timestamp=1591079632972, channel='#en.wikipedia', title='Delhi School of Economics', diffUrl='https://en.wikipedia.org/w/index.php?diff=960305816&oldid=960305706', user='Wiki8683', byteDiff=55, summary='/* Department of Commerce */', flags=0}
//        wiki日志形式：:3> WikipediaEditEvent{timestamp=1591079634613, channel='#en.wikipedia', title='Wikipedia:Reference desk/Science', diffUrl='https://en.wikipedia.org/w/index.php?diff=960305817&oldid=960297215', user='PaleCloudedWhite', byteDiff=477, summary='/* Is there a legal/fiscal/cultural reason why fields in Estonia might generally have a grove or orchard in them */ wooded meadows', flags=0}


        KeyedStream<WikipediaEditEvent, String> keyEdits = source.keyBy(new KeySelector<WikipediaEditEvent, String>() {
            @Override
            public String getKey(WikipediaEditEvent value) throws Exception {
                //按照用户名组织数据
                return value.getUser();
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Long>> result = keyEdits
                .timeWindow(Time.seconds(5))
                .fold(new Tuple2("", 0L), new FoldFunction<WikipediaEditEvent, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> fold(Tuple2<String, Long> accumulator, WikipediaEditEvent value) throws Exception {
                        accumulator.f0 = value.getUser();
                        accumulator.f1 += value.getByteDiff();
                        return accumulator;
                    }
                });

        result.print();
        env.execute("WikiEditsFlinkStreamingDemo");

    }

}
