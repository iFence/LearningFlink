package it.aspirin.kafka

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

import it.aspirin.utils.FlinkUtils
import org.apache.flink.api.common.functions.{IterationRuntimeContext, RichFunction, RuntimeContext}
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, FlinkKafkaProducer011}
import org.apache.flink.streaming.api.scala._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

/**
 * 测试读取kafka中的数据
 */
object KafkaDemo {
  def main(args: Array[String]): Unit = {
    val env = FlinkUtils.getStreamEnv
    val value = addKafkaSource(env)
    addConsoleSink(value)
    addKafkaSink(value)
    FlinkUtils.start(env)
  }

  /**
   * 添加kafka数据源
   *
   * @param env 流处理环境
   */
  def addKafkaSource(env: StreamExecutionEnvironment) = {
    val props = new Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink-demo")
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

    env.addSource(new FlinkKafkaConsumer[String]("test-old", new SimpleStringSchema(), props))
  }

  def addConsoleSink(dataStream: DataStream[String]): Unit = {
    dataStream.print("kafka")
  }

  def addKafkaSink(dataStream: DataStream[String]) = {
    dataStream.addSink(new FlinkKafkaProducer[String]("","",new SimpleStringSchema()))
  }

  def addESSink(dataStream: DataStream[String]) = {

  }

  def addJdbcSink(dataStream: DataStream[String]) = {

  }

}

case class SensorReading(id: String, time: Long, tem: Double)












