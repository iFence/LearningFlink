package it.aspirin.datasetAndStream

import org.apache.flink.streaming.api.scala._

object DataStreamDemo {
  def main(args: Array[String]): Unit = {
    val env = getEnv()
    val source = addSocketSource(env)
    val transformed = transform(source)
    addConsoleSink(transformed)
    start(env)
  }

  def getEnv(): StreamExecutionEnvironment = {
    StreamExecutionEnvironment.getExecutionEnvironment
  }

  def addSocketSource(env: StreamExecutionEnvironment): DataStream[String] = {
    env.socketTextStream("localhost", 9999)
  }

  def addTextSource(env: StreamExecutionEnvironment):DataStream[String] = {
    env.readTextFile("/Users/yulei/IdeaProjects/personal/LearningFlink/src/main/resources/emp.txt")
  }

  def transform(inputStream: DataStream[String]): DataStream[(String, Int)] = {
    inputStream
      .flatMap(_.split(","))
      .map((_, 1))
      //流式处理使用keyBy，批处理使用groupBy
      .keyBy(0)
      .sum(1)
  }

  def addConsoleSink(transformedStream: DataStream[(String, Int)]): Unit = {
    transformedStream.print()
  }

  def start(env:StreamExecutionEnvironment): Unit ={
    env.execute("DataStream demo")
  }

}
