package it.aspirin.kafka

import java.util.Properties

import it.aspirin.utils.FlinkUtils
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
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
    addSink(value)
    FlinkUtils.start(env)
  }

  /**
   * 添加kafka数据源
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

  def addSink(dataStream: DataStream[String]): Unit ={
    dataStream.print("kafka")
  }

}
