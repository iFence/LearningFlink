package it.aspirin.utils

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object FlinkUtils {

  /**
   * 获取流处理环境
   * @return
   */
  def getStreamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  /**
   * 启动程序
   *
   * @param env 程序环境
   */
  def start(env: StreamExecutionEnvironment): Unit = {
    env.execute("DataStream demo")
  }

  def start(env: StreamExecutionEnvironment, name:String): Unit = {
    env.execute(name)
  }

}
