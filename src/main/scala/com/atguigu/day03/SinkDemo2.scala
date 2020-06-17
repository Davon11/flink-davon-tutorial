package com.atguigu.day03

import java.util.Properties
import com.atguigu.day01.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}


/**
 * @description: 从kafka读取数据，经flink处理后写入kafka
 * @author: Davon
 * @created: 2020/06/10 20:49
 */
object SinkDemo2 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val prop = new Properties()
    prop.put("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop104:9092")
    prop.put("group.id","flink_consumer")
    prop.put(
      "key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer"
    )
    prop.put(
      "value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer"
    )
    prop.put("auto.offset.reset", "latest")
    env.setParallelism(1)
    val kafkaStream: DataStream[String] = env.addSource(
      new FlinkKafkaConsumer011[String](
        "source_demo",
        new SimpleStringSchema(),
        prop))
    val sensorStream: DataStream[String] = kafkaStream.map(date => {
      val strings: Array[String] = date.split(",")
      SensorReading(
        strings(0), strings(1).toLong, strings(2).toDouble)
        .toString
    })
    sensorStream.addSink(
      new FlinkKafkaProducer011[String](
        "hadoop102:9092,hadoop103:9092,hadoop104:9092",
        "sink_demo",
        new SimpleStringSchema()))

    sensorStream.print()
    env.execute("sink_demo")
  }

}
