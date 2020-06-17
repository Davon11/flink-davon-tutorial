package com.atguigu.day06

import com.atguigu.day01.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @description: ${description}
 * @author: Davon
 * @created: 2020/06/16 13:57
 */
object CosFlatMapFunctionDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env
      .addSource(new SensorSource)
      .keyBy(_.sensorId)
      .flatMap(new MyFlatMap)
      .print()
    env.execute()
  }

  class MyFlatMap extends RichFlatMapFunction[SensorReading, String] {
    private var lastSensor: ValueState[SensorReading] = _
    private var notFirst: ValueState[Boolean] = _


    override def open(parameters: Configuration): Unit = {
      lastSensor = getRuntimeContext.getState(
        new ValueStateDescriptor[SensorReading]("last_sensor", classOf[SensorReading])
      )
      notFirst = getRuntimeContext.getState(
        new ValueStateDescriptor[Boolean]("not_fist", classOf[Boolean])
      )
    }

    override def flatMap(in: SensorReading, collector: Collector[String]): Unit = {
      var diff = 0D
      if (notFirst.value()){
        diff = (in.temperature - lastSensor.value().temperature).abs
      }else {
        notFirst.update(true)
      }
      if (diff > 1.4){
        collector.collect("warn :"  + (in.sensorId, in.temperature, diff))
      }
      lastSensor.update(in)
    }
  }
}
