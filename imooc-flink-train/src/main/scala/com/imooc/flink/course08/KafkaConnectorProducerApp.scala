package com.imooc.flink.course08

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper

/**
 * @ClassName: KafkaConnectorProducerApp
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/2/10 12:59 下午
 */

object KafkaConnectorProducerApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    val data = env.socketTextStream("localhost", 9999)
    data.print()

    val topic = "pktest"
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.199.233:9092")

    val kafkaSink = new FlinkKafkaProducer[String](topic, new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()), properties)

    data.addSink(kafkaSink)
    env.execute("KafkaConnectorProducerApp")
  }

}
