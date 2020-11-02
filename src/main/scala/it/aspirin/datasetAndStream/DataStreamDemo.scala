package it.aspirin.datasetAndStream

import it.aspirin.utils.FlinkUtils
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object DataStreamDemo {
  def main(args: Array[String]): Unit = {
    val env = FlinkUtils.getStreamEnv
    val params = ParameterTool.fromArgs(args)
//    val hostname = params.get("host")
//    val port = params.getInt("port")
//    val source = addSocketSource(env, hostname, port)
    val source = addTextSource(env)
    val transformed = transform(source)
    addConsoleSink(transformed)
    FlinkUtils.start(env)
  }

  /**
   * 添加socket源
   *
   * @param env 流处理环境
   * @return
   */
  def addSocketSource(env: StreamExecutionEnvironment, hostname: String, port: Int): DataStream[String] = {
    env.socketTextStream(hostname, port)
  }

  /**
   * 添加文件源
   *
   * @param env 流处理环境
   * @return
   */
  def addTextSource(env: StreamExecutionEnvironment): DataStream[String] = {
    env.readTextFile("/Users/yulei/IdeaProjects/personal/LearningFlink/src/main/resources/emp.txt")
  }

  /**
   * 对数据进行转换
   *
   * @param inputStream 输入流
   * @return
   */
  def transform(inputStream: DataStream[String]): DataStream[(String, Int)] = {
    inputStream
      .flatMap(_.split(","))
      .filter(_.nonEmpty)
      .map((_, 1)).setParallelism(6)
      //流式处理使用keyBy，批处理使用groupBy
      .keyBy(0)
      .sum(1)
  }

  /**
   * 添加控制台输出
   *
   * @param transformedStream 转换好的流
   */
  def addConsoleSink(transformedStream: DataStream[(String, Int)]): Unit = {
    transformedStream.print("stream1")
    transformedStream.print("stream2")
  }

}
