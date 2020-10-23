package it.aspirin.table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Json, Kafka, Schema}
import org.apache.kafka.clients.consumer.ConsumerConfig

object FlinkKafkaDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)

    //在catalog创建表
    createKafkaSourceTable(tableEnv)
//    createKafkaSinkTable(tableEnv)
//    createCSVSinkTable(tableEnv)

    //table转换逻辑，下面用到了单引号，是在隐式转换中的内容，所以一定要把隐式转换的内容引进来(当然也可以不使用单引号)
    val tableTransedTable = doTableTrans(tableEnv)
    //将聚合数据写入到MySQL中
    jdbcSink(tableEnv,tableTransedTable)

    //聚合不是追加流，所以这里不能写追加
    //tableTransTable.toRetractStream[(String, Long)].print("table")

    //使用SQL api做逻辑转换
    //val transSQLTable = doSQLTrans(tableEnv)
    //同时sink到csv和kafka
    //doSink(transSQLTable)

    //ds/table/view的转换操作
//    val dataStream: DataStream[(String, String)] = transSQLTable.toAppendStream[(String, String)]
//    transSQLTable.toAppendStream[(String, String)].print("sql table")

    //代码执行
    tableEnv.execute("flink mysql demo")
    //env.execute("flink mysql demo")
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
      .createTemporaryTable("kafka_input_table")
  }

  def createKafkaSinkTable(tableEnv: StreamTableEnvironment) = {
    //连接kafka
    tableEnv.connect(new Kafka()
      .version("universal")
      .topic("test-sink")
      .property("zookeeper.connect", "localhost:2181")
      .property("bootstrap.servers", "localhost:9092")
      .property(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"))
      //将kafka中的数据格式化为json数据
      .withFormat(new Json())
      //将json数据转为table
      .withSchema(new Schema()
        .field("site", DataTypes.STRING())
        .field("user", DataTypes.STRING()))
      .createTemporaryTable("kafka_sink_table")
  }

  def createCSVSinkTable(tableEnv: StreamTableEnvironment) = {
    tableEnv.connect(new FileSystem()
      .path("/Users/yulei/IdeaProjects/personal/LearningFlink/src/main/resources/csv"))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("site", DataTypes.STRING())
        .field("user", DataTypes.STRING()))
      .createTemporaryTable("csv_sink_table")
  }

  def doTableTrans(tableEnv: StreamTableEnvironment) = {
    tableEnv.from("kafka_input_table")
      .select('site, 'user, 'time)
      .groupBy('user)
      .select('user as 'user_1, 'user.count as 'count_1)
  }

  def doSQLTrans(tableEnv: StreamTableEnvironment) = {
    tableEnv.sqlQuery(
      """
        |select site, user
        |from kafka_input_table
        |where user = 'Bob'
        |""".stripMargin)
  }

  def doSink(sinkTable: Table) = {

    sinkTable.insertInto("kafka_sink_table")
    sinkTable.insertInto("csv_sink_table")
  }

  /**
   * Flink sink to jdbc 跟sink到其他系统差别比较大，她更加接近于传统SQL ddl语言
   */
  def jdbcSink(tableEnv: StreamTableEnvironment,result: Table) = {
    //解释一下的是create table语句中的sink_agg是注册到flink中的表，数据写出到外部系统的user_pv表中
    val sinkDDL:String =
      """
        |create table kafka_sink(
        |user_1 varchar(100) not null,
        |count_1 bigint not null
        |) with (
        | 'connector.type' = 'jdbc',
        | 'connector.url' = 'jdbc:mysql://localhost:3306/aspirin',
        | 'connector.table' = 'user_pv1',
        | 'connector.driver' = 'com.mysql.cj.jdbc.Driver',
        | 'connector.username' = 'root',
        | 'connector.password' = 'root'
        |)
        |""".stripMargin
    tableEnv.sqlUpdate(sinkDDL)//执行DDL建表
    result.insertInto("kafka_sink")
  }
}
