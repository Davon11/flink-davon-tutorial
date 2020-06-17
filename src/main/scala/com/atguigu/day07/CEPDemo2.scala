package com.atguigu.day07

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.Map

/**
 * @description: ${description}
 * @author: Davon
 * @created: 2020/06/17 09:10
 */
object CEPDemo2 {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val UserStream = env.socketTextStream("localhost", 9999)
      .map(
        line => {
          val strings: Array[String] = line.split(" ")
          (strings(0), strings(1).toLong, strings(2))
        }
      )
      .assignAscendingTimestamps(_._2)
    val pattern: Pattern[(String, Long, String), (String, Long, String)] = Pattern
      .begin[(String, Long ,String)]("first").where(_._3 == "1")
      .next("second").where(_._3 == "2")
      .next("third").where(_._3 == "3")
      .within(Time.minutes(1))
    CEP
      .pattern(UserStream, pattern)
      .select(func)
      .print()
    env.execute("ecp demo")

  }
  val func = (pattern: Map[String, Iterable[(String, Long, String)]]) => {
//    val first: (String, Long, String) = pattern.getOrElse("first", null).iterator.next()
//    val second: (String, Long, String) = pattern.getOrElse("second", null).iterator.next()
//    val third: (String, Long, String) = pattern.getOrElse("third", null).iterator.next()
    val first: (String, Long, String) = pattern("first").head
    val second: (String, Long, String) = pattern("second").head
    val third: (String, Long, String) = pattern("third").head
    (first, second, third)
  }

}
