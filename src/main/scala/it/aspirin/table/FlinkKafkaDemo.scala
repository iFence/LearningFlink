package it.aspirin.table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Json, Kafka, Schema}
import org.apache.kafka.clients.consumer.ConsumerConfig

object FlinkKafkaDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)

    //连接kafka
    tableEnv.connect(new Kafka()
      .version("universal")
      .topic("test-old")
      .property("zookeeper.connect", "localhost:2181")
      .property("bootstrap.servers", "localhost:9092")
      .property(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    )
      //将kafka中的数据格式化为json数据
      .withFormat(new Json())
      //将json数据转为table
      .withSchema(new Schema()
        .field("site", DataTypes.STRING())
        .field("time", DataTypes.STRING())
        .field("user", DataTypes.STRING()))
      .createTemporaryTable("kafka_input_table")

    //转换逻辑，下面用到了单引号，是在隐式转换中的内容，所以一定要把隐式转换的内容引进来
    tableEnv.from("kafka_input_table")
      .select('site, 'user, 'time)
      .groupBy('user)
      .select('user, 'user.count as 'count)
      .toRetractStream[(String, Long)]
      .print()

    env.execute("flink kafka demo")
  }
}
