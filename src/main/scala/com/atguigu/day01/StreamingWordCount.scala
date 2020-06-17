package com.atguigu.day01

import org.apache.flink.streaming.api.scala._

object StreamingWordCount {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val host = "hadoop103"
    val port = 9999
    val inputStream: DataStream[String] = env.socketTextStream(host,port)
    val resultStream: DataStream[(String, Int)] = inputStream
      .flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .sum(1)
    resultStream.print()
    env.execute()
  }

}
