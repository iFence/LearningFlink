package it.aspirin.table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._

case class Emp(name: String, id: Long, salary: Double)

/**
 * 测试flink table api 和SQL API的使用
 */
object TableDemo1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream = env.socketTextStream("localhost", 9999)

    //将流转成样例类
    val dataStream = inputStream.map(data => {
      val dataArray = data.split(",")
      Emp(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
    })

    //1.基于env创建表的执行环境
    val tableEnv = StreamTableEnvironment.create(env)

    //2.基于tableEnv将流转换成表
    val dataTable: Table = tableEnv.fromDataStream(dataStream)
    //3.调用table api做转换操作
    val resultTable = dataTable
      .select("name, salary")
      .filter("salary > 5000")
    //table不能直接打印，所以把表转成流打印输出
    resultTable.toAppendStream[(String,Double)].print("TABLE API")

    //基于SQL的方式实现转换
    tableEnv.createTemporaryView("emp", dataTable)//将流注册成表
    //写SQL进行计算并转换成流进行输出
    tableEnv.sqlQuery("select id, name, salary from emp where salary > 2000").toAppendStream[(Long,String,Double)].print("SQL API")

    env.execute("table api demo")
  }
}
