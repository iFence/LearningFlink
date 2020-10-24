package it.aspirin.table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Elasticsearch, Json, Kafka, Schema}
import org.apache.kafka.clients.consumer.ConsumerConfig

/**
 * flink table sink to es test
 */
object ESSinkDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)

    createKafkaSourceTable(tableEnv)
    createEsSinkTable(tableEnv)
    val resTable = doTableTrans(tableEnv)
    doSink(resTable)

//    env.execute("es flink demo")
    tableEnv.execute("es flink demo")
  }

  def createKafkaSourceTable(tableEnv: StreamTableEnvironment) = {
    //连接kafka
    tableEnv.connect(new Kafka()
      .version("universal")
      .topic("test-old")
      .property("zookeeper.connect", "localhost:2181")
      .property("bootstrap.servers", "localhost:9092")
      .property(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"))
      //将kafka中的数据格式化为json数据
      .withFormat(new Json())
      //将json数据转为table
      .withSchema(new Schema()
        .field("site", DataTypes.STRING())
        .field("time", DataTypes.STRING())
        .field("user", DataTypes.STRING()))
      .createTemporaryTable("kafka_table")
  }

  def createEsSinkTable(tableEnv: StreamTableEnvironment) = {
    tableEnv.connect(
      new Elasticsearch()
        .version("7")
        .host("localhost",9200,"http")
        .index("sensor_flink")
        .documentType("_doc")
    )
      .inUpsertMode()
      .withFormat(new Json())
      .withSchema(new Schema().field("user",DataTypes.STRING())
      .field("count",DataTypes.BIGINT()))
      .createTemporaryTable("esOutputTable")
  }

  def doTableTrans(tableEnv: StreamTableEnvironment) = {
    tableEnv.from("kafka_table")
      .select('site, 'user, 'time)
      .groupBy('user)
      .select('user as 'user, 'user.count as 'count)
  }

  def doSink(res:Table) = {
    res.insertInto("esOutputTable")
  }
}
