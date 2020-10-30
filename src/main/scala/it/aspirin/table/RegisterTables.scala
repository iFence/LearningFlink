package it.aspirin.table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.descriptors.{Csv, Elasticsearch, FileSystem, Json, Kafka, Schema}
import org.apache.kafka.clients.consumer.ConsumerConfig

/**
 * 将各种需要的数据源注册到catalog中
 */
object RegisterTables {

  /**
   * 注册 es table
   *
   * @param tableEnv  表环境
   * @param tableName 表名称
   */
  def createEsSinkTable(tableEnv: StreamTableEnvironment, tableName: String) = {
    tableEnv.connect(
      new Elasticsearch()
        .version("7")
        .host("localhost", 9200, "http")
        .index("sensor_flink")
        .documentType("_doc")
    )
      .inUpsertMode()
      .withFormat(new Json())
      .withSchema(new Schema().field("user", DataTypes.STRING())
        .field("count", DataTypes.BIGINT()))
      .createTemporaryTable(tableName)
  }

  /**
   * sql方式注册表
   * 这种方式有问题
   * @param tableEnv
   * @param tableName
   */
  def createSQLEsSinkTable(tableEnv: StreamTableEnvironment, tableName: String) = {
    val esSQLTable =
      s"""
        |CREATE TABLE $tableName (
        |  user_1 String,
        |  count_1 BIGINT
        |) WITH (
        |  'connector' = '7',
        |  'hosts' = 'http://localhost:9200',
        |  'index' = 'flink_users'
        |)
        |""".stripMargin
    tableEnv.sqlUpdate(esSQLTable)
  }

  /**
   * 创建kafka table
   *
   * @param tableEnv  表环境
   * @param tableName 表名
   */
  def createKafkaTable(tableEnv: StreamTableEnvironment, tableName: String) = {
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
      .createTemporaryTable(tableName)
  }

  /**
   * 创建csv table
   * @param tableEnv
   * @param tableName
   */
  def createCSVSinkTable(tableEnv: StreamTableEnvironment, tableName: String) = {
    tableEnv.connect(new FileSystem()
      .path("/Users/yulei/IdeaProjects/personal/LearningFlink/src/main/resources/csv"))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("user_1", DataTypes.STRING())
        .field("count_1", DataTypes.BIGINT()))
      .createTemporaryTable(tableName)
  }

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
//    result.insertInto("kafka_sink")
  }

}
