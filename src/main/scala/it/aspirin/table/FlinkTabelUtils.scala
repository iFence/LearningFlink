package it.aspirin.table

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment

/**
 * Flink Table/SQL相关功能工具类
 */
object FlinkTabelUtils {

  def getEnv() = StreamExecutionEnvironment.getExecutionEnvironment

  def getTableEnv(env: StreamExecutionEnvironment) = StreamTableEnvironment.create(env)
}
