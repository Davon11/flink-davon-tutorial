package com.atguigu.day07

import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.Map

/**
 * @description: ${description}
 * @author: Davon
 * @created: 2020/06/17 13:56
 */
object OrderTimeout {
  case class OrderEvent(orderId: String, eventType: String, eventTime: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env
      .fromElements(
        OrderEvent("order_1", "create", 2000L),
        OrderEvent("order_2", "create", 3000L),
        OrderEvent("order_2", "pay", 4000L)
      )
      .assignAscendingTimestamps(_.eventTime)
      .keyBy(_.orderId)
    val pattern: Pattern[OrderEvent, OrderEvent] = Pattern
      .begin[OrderEvent]("first").where(_.eventType.equals("create"))
      .followedBy("second").where(_.eventType.equals("pay"))
      .within(Time.seconds(5))
    val patternStream: PatternStream[OrderEvent] = CEP.pattern(stream, pattern)

   /*  def flatSelect[L: TypeInformation, R: TypeInformation](outputTag: OutputTag[L])(
    patternFlatTimeoutFunction: (Map[String, Iterable[T]], Long, Collector[L]) => Unit) (
    patternFlatSelectFunction: (Map[String, Iterable[T]], Collector[R]) => Unit)
  : DataStream[R] = {*/
    val tag: OutputTag[String] = OutputTag[String]("timeout_OrderEvent")
    val timeoutFunc = (pat: Map[String, Iterable[OrderEvent]], ts: Long, coll: Collector[String]) =>{
      val timeoutEvent: OrderEvent = pat.getOrElse("first", null).head
      val alert: String = timeoutEvent.orderId + "迟到了"
      coll.collect(alert)
    }
    val timelyFunc= (pat: Map[String, Iterable[OrderEvent]], coll: Collector[String]) => {
      val timeoutEvent: OrderEvent = pat.getOrElse("first", null).head
      val alert: String = timeoutEvent.orderId + "及时到达"
      coll.collect(alert)
    }

    val selectedStream: DataStream[String] = patternStream.flatSelect(tag)(timeoutFunc)(timelyFunc)
    selectedStream.print()
    selectedStream.getSideOutput(OutputTag[String]("timeout_OrderEvent")).print()

//    stream.print()
    env.execute()

  }

}
