package it.aspirin.table

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Elasticsearch, FileSystem, Json, Kafka, Schema}
import org.apache.kafka.clients.consumer.ConsumerConfig

/**
 * Flink Table/SQL相关功能工具类
 */
object FlinkTableUtils {

  def getEnv() = StreamExecutionEnvironment.getExecutionEnvironment

  def getTableEnv(env: StreamExecutionEnvironment) = StreamTableEnvironment.create(env)

}
