package it.aspirin.customize

import java.sql.{Connection, DriverManager, PreparedStatement}

import it.aspirin.utils.FlinkUtils
import org.apache.flink.api.common.functions.{IterationRuntimeContext, RuntimeContext}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingProcessingTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

import scala.util.Random

/**
 * 实现自定义的source和sink
 */
object CustomizeDemo {
  def main(args: Array[String]): Unit = {
    val env = FlinkUtils.getStreamEnv
    val value = addCustomizeSource(env)
    addConsoleSink(value)
    //    addJdbcSink(value)
    FlinkUtils.start(env)
  }

  def addCustomizeSource(env: StreamExecutionEnvironment): DataStream[SensorReading] = {
    env.addSource(new MySource())
  }

  /**
   * window转换操作
   */
  def windowTrans(sourceStream: DataStream[SensorReading]) = {
    sourceStream.keyBy("id")
      //      .window(TumblingEventTimeWindows.of(Time.seconds(15))) //滚动事件时间窗口
      //      .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5))) //滑动处理时间窗口
      //      .timeWindow(Time.seconds(15)) //滚动窗口的简写形式，具体是滑动还是事件时间还是处理时间程序会自己判断
//      .timeWindow(Time.seconds(15), Time.seconds(10)) //滑动窗口的简写，具体是滑动还是事件时间还是处理时间程序会自己判断
//      .countWindow(10L) //滚动窗口
      .countWindow(10L, 5L)//滑动窗口
  }

  def addConsoleSink(dataStream: DataStream[SensorReading]): Unit = {
    dataStream.print()
  }

  def addJdbcSink(dataStream: DataStream[SensorReading]): Unit = {
    dataStream.addSink(new MySQLSink)
  }

}

case class SensorReading(id: String, time: Long, tem: Double)

class MySource extends RichParallelSourceFunction[SensorReading] {

  //定义一个标志位，表示数据源是否正常运行
  var running = true

  override def setRuntimeContext(t: RuntimeContext): Unit = super.setRuntimeContext(t)

  override def open(parameters: Configuration): Unit = super.open(parameters)

  override def close(): Unit = super.close()

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    val rand = new Random()

    var currTemps = 1 to 10 map (i => (s"sensor_$i", 60 + rand.nextGaussian() * 20))

    //无限循环，生成随机数值
    while (running) {
      currTemps = currTemps.map(data => (data._1, data._2 + rand.nextGaussian()))
      val currTs = System.currentTimeMillis()
      currTemps.foreach(data => sourceContext.collect(SensorReading(data._1, currTs, data._2)))
      Thread.sleep(500)
    }


  }

  override def cancel(): Unit = false
}

class MySQLSink extends RichSinkFunction[SensorReading] {
  var connection: Connection = _
  var insertStatement: PreparedStatement = _
  var updateStatement: PreparedStatement = _

  override def open(configuration: Configuration): Unit = {

    connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "root")
    insertStatement = connection.prepareStatement("insert into temp (sensor, temp) values (?, ?)")
    updateStatement = connection.prepareStatement("update temp set temp = ? where sensor = ?")

  }

  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    //执行更新语句，如果没有语句被更新说明需要执行插入语句
    updateStatement.setDouble(1, value.tem)
    updateStatement.setString(2, value.id)
    updateStatement.execute()

    //如果没有执行更新语句，执行插入
    if (updateStatement.getUpdateCount == 0) {
      insertStatement.setString(1, value.id)
      insertStatement.setDouble(2, value.tem)
      insertStatement.execute()
    }
  }

  override def close(): Unit = {
    connection.close()
  }
}
