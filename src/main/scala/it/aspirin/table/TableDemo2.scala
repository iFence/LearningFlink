package it.aspirin.table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

object TableDemo2 {
  val sourceFile = "/Users/yulei/IdeaProjects/personal/LearningFlink/src/main/resources/emp.txt"
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnvironment = StreamTableEnvironment.create(env)

    //创建一张表，用于读取数据，相当于source
    tableEnvironment
      .connect(new FileSystem().path(sourceFile))
      .withFormat(new Csv()) //定义了从外部文件格式化数据的方法
      .withSchema(new Schema()
        .field("name", DataTypes.STRING())
        .field("id", DataTypes.BIGINT())
        .field("salary", DataTypes.DOUBLE()))
      .createTemporaryTable("inputTable")
    //注册一张表，用于写入数据，相当于sink
//    tableEnvironment.connect().createTemporaryTable("outputTable")

    //通过table查询算子，得到一张结果表
    val tabRes = tableEnvironment.from("inputTable").select("name, id, salary").filter("salary > 7000")
    tabRes.toAppendStream[(String,Long,Double)].print()

    //通过SQL查询语句得到结果表
    val sqlRes = tableEnvironment.sqlQuery("select name from inputTable")
//    tabRes.insertInto("outputTable")
    env.execute("table & sql api demo")

  }

}
