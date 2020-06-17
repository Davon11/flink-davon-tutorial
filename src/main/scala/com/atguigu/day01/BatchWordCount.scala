package com.atguigu.day01

import org.apache.flink.api.scala._

/**
 * @description: ${description}
 * @author: Davon
 * @created: 2020/06/08 22:43
 */
object BatchWordCount {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val inputSet: DataSet[String] = env.readTextFile("F:\\Work\\IDEAWorkSpace\\" +
      "flink-tutorial-student\\src\\main\\resources\\words.txt")
    val resultSet: AggregateDataSet[(String, Int)] = inputSet
      .flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)
    resultSet.print()
//    env.execute()
  }

}
