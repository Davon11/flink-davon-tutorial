package com.atguigu.day07

import org.apache.flink.cep.scala.CEP
import scala.collection.Map
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @description: ${description}
 * @author: Davon
 * @created: 2020/06/17 09:10
 */
object CEPDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val UserStream = env.fromElements(
      UserLogin("user1", "0.0.0.1", "fail", 1000L),
      UserLogin("user1", "0.0.0.2", "fail", 2000L),
      UserLogin("user1", "0.0.0.3", "success", 3000L),
      UserLogin("user1", "0.0.0.1", "fail", 4000L),
      UserLogin("user1", "0.0.0.2", "fail", 5000L),
      UserLogin("user1", "0.0.0.1", "fail", 6000L)
    )
      .assignAscendingTimestamps(_.timestamp)
    val pattern: Pattern[UserLogin, UserLogin] = Pattern
      .begin[UserLogin]("first").where(_.state.equals("fail"))
      .next("second").where(_.state.equals("fail"))
      .next("third").where(_.state.equals("fail"))
      .within(Time.seconds(10))
    CEP
      .pattern(UserStream, pattern)
      .select(func)
      .print()
    env.execute("ecp demo")

  }
  val func = (pattern: Map[String, Iterable[UserLogin]]) => {
    val first: UserLogin = pattern.getOrElse("first", null).iterator.next()
    val second: UserLogin = pattern.getOrElse("second", null).iterator.next()
    val third: UserLogin = pattern.getOrElse("third", null).iterator.next()
    (first.userID, first.ip, second.ip, third.ip)
  }

  case class UserLogin(userID: String, ip: String, state: String, timestamp: Long)

}
