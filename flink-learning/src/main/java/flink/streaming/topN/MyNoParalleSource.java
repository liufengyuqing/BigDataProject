package flink.streaming.topN;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.Random;

/**
 * create by liuzhiwei on 2020/4/27
 * <p>
 * 一个并行度为1的发送器，用来向kafka发送数据：
 */
public class MyNoParalleSource implements SourceFunction<String> {
    private boolean isRunning = true;

    /**
     * 主要的方法
     * 启动一个source
     * 大部分情况下，都需要在这个run方法中实现一个循环，这样就可以循环产生数据了
     * <p>
     * 每过1秒向Kafka的topn这个topic随机发送一本书的名字用来模拟购买行为。
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        ArrayList<String> books = new ArrayList<>();
        books.add("Pyhton从入门到放弃");//10
        books.add("Java从入门到放弃");//8
        books.add("Php从入门到放弃");//5
        books.add("C++从入门到放弃");//3
        books.add("Scala从入门到放弃");//0-4

        while (isRunning) {
            int i = new Random().nextInt(5);
            ctx.collect(books.get(i));

            //每1秒产生一条数据
            Thread.sleep(1000);
        }

    }

    //取消一个cancel的时候会调用的方法
    @Override
    public void cancel() {
        isRunning = false;
    }
}
