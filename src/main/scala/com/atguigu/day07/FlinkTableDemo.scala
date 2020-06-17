package com.atguigu.day07

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

/**
 * @description: ${description}
 * @author: Davon
 * @created: 2020/06/17 16:41
 */
object FlinkTableDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    tableEnv
        .connect(new FileSystem().path("F:\\Work\\IDEAWorkSpace\\flink-tutorial-student\\src\\main\\resources\\sensor.date"))
        .withFormat(new Csv)
        .withSchema(
          new Schema()
            .field("id", DataTypes.STRING())
            .field("timestamp", DataTypes.BIGINT())
            .field("temperature", DataTypes.DOUBLE())
        )
        .createTemporaryTable("sensor_table")

    val sensorTable: Table = tableEnv.from("sensor_table")
    val filteredTable: Table = sensorTable
      .select("id, temperature")
      .filter("id = 'sensor_6'")

    filteredTable



    env.execute()
  }

}
