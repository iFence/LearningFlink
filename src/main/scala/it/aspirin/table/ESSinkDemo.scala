package it.aspirin.table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala.StreamTableEnvironment


/**
 * flink table sink to es test
 */
object ESSinkDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)

    RegisterTables.createKafkaTable(tableEnv, "t_source")
    RegisterTables.createEsSinkTable(tableEnv, "t_es_sink")
    RegisterTables.createCSVSinkTable(tableEnv, "csv_sink")

    val resTable = doTableTrans(tableEnv, "t_source")
    doSink(resTable)

    env.execute("es flink demo")
  }


  def doTableTrans(tableEnv: StreamTableEnvironment, sourceTable: String) = {
    tableEnv.from(sourceTable)
      .select('site, 'user, 'time)
      .groupBy('user)
      .select('user as 'user_1, 'user.count as 'count_1)
  }

  def doSink(res: Table) = {
    res.insertInto("t_es_sink")
    res.insertInto("csv_sink")
  }
}
