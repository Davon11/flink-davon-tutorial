package com.atguigu.day06

import com.atguigu.day01.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.scala._

/**
 * @description: ${description}
 * @author: Davon
 * @created: 2020/06/16 14:17
 */
object CosFlatmapWithStateDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env
      .addSource(new SensorSource)
      .keyBy(_.sensorId)
      // 第一个参数是输出的元素的类型，第二个参数是状态变量的类型
      .flatMapWithState[(String,Double,Double),Double]{
        case (in: SensorReading, None) => {
          (List.empty, Some(in.temperature))
        }
        case (in: SensorReading, lastTemp: Some[Double]) => {
          val tempDiff = (in.temperature - lastTemp.get).abs
          if (tempDiff > 1.4){
            (List((in.sensorId,in.temperature,tempDiff)), Some(in.temperature))
          }else {
            (List.empty,Some(in.temperature))
          }
        }
      }
      .print()
    env.execute()
  }

}
