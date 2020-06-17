package com.atguigu.day05

import com.atguigu.day01.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object IntervalJoinExample2 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
//    1592354240441

    val clickStream = env
      .addSource(new SensorSource)
      .assignAscendingTimestamps(_.timeStamp * 1000L)
      .keyBy(_.sensorId)

    val browseStream = env
      .addSource(new SensorSource)
      .assignAscendingTimestamps(_.timeStamp * 1000L)
      .keyBy(_.sensorId)

    clickStream
      .intervalJoin(browseStream)
      .between(Time.seconds(-10), Time.seconds(10))
      .process(new MyIntervalJoin)
      .print()

    env.execute()
  }

  class MyIntervalJoin extends ProcessJoinFunction[SensorReading, SensorReading, String] {
    override def processElement(left: SensorReading, right: SensorReading, ctx: ProcessJoinFunction[SensorReading, SensorReading, String]#Context, out: Collector[String]): Unit = {
      out.collect(left + " ==> " + right)
    }
  }
}
