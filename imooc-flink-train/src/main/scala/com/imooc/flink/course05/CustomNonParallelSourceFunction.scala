package com.imooc.flink.course05

import org.apache.flink.streaming.api.functions.source.SourceFunction

/**
 * @ClassName: CustomNonParallelSourceFunction
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/2/8 10:10 下午
 */

class CustomNonParallelSourceFunction extends SourceFunction[Long] {
  var count = 1L

  var isRunning = true

  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while (isRunning) {
      ctx.collect(count)
      count += 1
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}
