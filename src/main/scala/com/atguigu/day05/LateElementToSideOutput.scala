package com.atguigu.day05

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @description: ${description}
 * @author: Davon
 * @created: 2020/06/16 13:57
 */
object LateElementToSideOutput {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val resultStream: DataStream[String] = env
      .socketTextStream("localhost", 9999)
      .map(
        line => {
          val strings: Array[String] = line.split(" ")
          (strings(0), strings(1).toLong)
        }
      )
      .assignAscendingTimestamps(_._2 * 1000)
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .sideOutputLateData(
        new OutputTag[(String, Long)]("lateDate")
      )
      .process(new WindowCloseAlert)
    resultStream.print()
    resultStream.getSideOutput(OutputTag[(String,Long)]("lateDate")).print()
    env.execute("LateElementToSideOutput")
  }
}
class WindowCloseAlert extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
    out.collect(context.window.getStart + " 到 " + context.window.getEnd + "窗口关闭了")
  }
}
