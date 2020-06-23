package it.aspirin.table

import org.apache.flink.streaming.api.scala._

object DataStream2Table {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputDataStream = env.readTextFile("/Users/yulei/IdeaProjects/personal/LearningFlink/src/main/resources/emp.txt")

    inputDataStream
      .flatMap(_.split(","))
      .map((_, 1))
      //流式处理使用keyBy，批处理使用groupBy
      .keyBy(0)
      .sum(1)
      .print()

    inputDataStream.flatMap(_.split(","))

    env.execute("stream ")
  }

}
