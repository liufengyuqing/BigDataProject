package com.imooc.flink.course05;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @ClassName: JavaCustomNonParallelSourceFunction
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/2/8 10:31 下午
 */

public class JavaCustomNonParallelSourceFunction implements SourceFunction<Long> {

    boolean isRunning = true;
    long count = 1;

    @Override
    public void run(SourceContext ctx) throws Exception {
        while (true) {
            ctx.collect(count);
            count += 1;
            Thread.sleep(1000);

        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
