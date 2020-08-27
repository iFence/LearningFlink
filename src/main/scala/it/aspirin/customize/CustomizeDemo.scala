package it.aspirin.customize

import it.aspirin.utils.FlinkUtils
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

import scala.util.Random

/**
 * 实现自定义的source和sink
 */
object CustomizeDemo {
  def main(args: Array[String]): Unit = {
    val env = FlinkUtils.getStreamEnv
    val value = addCustomizeSource(env)
    addSink(value)
    FlinkUtils.start(env)
  }

  def addCustomizeSource(env: StreamExecutionEnvironment): DataStream[String] = {
    env.addSource(new MySource()).map(sensor => sensor.id + " " + sensor.tem + " " + sensor.time)
  }

  def addSink(dataStream: DataStream[String]): Unit ={
    dataStream.print()
  }
}

case class SensorReading(id: String, time: Long, tem: Double)

class MySource extends RichParallelSourceFunction[SensorReading] {

  //定义一个标志位，表示数据源是否正常运行
  var running = true

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

