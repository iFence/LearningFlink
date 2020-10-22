package it.aspirin.processFunc

import it.aspirin.processFunc.ProcessFuncDemo1.SensorReading
import it.aspirin.utils.FlinkUtils
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ProcessFuncDemo1 {
  def main(args: Array[String]): Unit = {
    val env = FlinkUtils.getStreamEnv
    val sourceStream = addSource(env)
//    val sinkStream = addTrans(sourceStream)
//    addConsoleSink(sinkStream)
    FlinkUtils.start(env, "process func test1")
  }

  case class SensorReading(id: Long, sensorName: String, temp: Double)

  def addSource(env: StreamExecutionEnvironment): DataStream[String] = {
    env.socketTextStream("localhost", 9999)
  }

//  def addTrans(sourceStream: DataStream[String]) = {
//    sourceStream.map(data => {
//      val arr = data.split(",")
//      SensorReading(arr(0).toLong, arr(1), arr(2).toDouble)
//    }).keyBy(_.id)
//      .process(new MyKeyedProcessFunc)
//  }

  /**
   * 定义侧输出流
   */
//  def addSideOutputTrans(sourceStream: DataStream[String]) = {
//    sourceStream.map(data => {
//      val arr = data.split(",")
//      SensorReading(arr(0).toLong, arr(1), arr(2).toDouble)
//    }).process(new SplitStream[SensorReading](30.0))
//  }

  def addConsoleSink(sinkStream: DataStream[SensorReading]) = {
    sinkStream.print("high") //高温流
    sinkStream.getSideOutput(new OutputTag[(Long, String, Double)]("low")) //低温流
  }

}

/**
 * 测试process function基本功能
 */
//class MyKeyedProcessFunc extends KeyedProcessFunction[Long, SensorReading, SensorReading] {
//  override def processElement(i: SensorReading, context: KeyedProcessFunction[Long, SensorReading, SensorReading]#Context, collector: Collector[String]): Unit = {
//    val currentKey = context.getCurrentKey
//    val ts = context.timestamp()
//    context.timerService().currentWatermark()
//    //注册定时器，可以注册多个，区分多个定时器依靠当前时间戳，所有定时器都会触发下面的onTimer方法执行
//    context.timerService().registerProcessingTimeTimer(context.timestamp() + 60000L)
//    //删除时间戳对应的定时器
//    context.timerService().deleteEventTimeTimer(context.timestamp())
//
//  }
//
//  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = super.onTimer(timestamp, ctx, out)
//}

/**
 * 测试分流操作
 */
//下面泛型中，第二个泛型是主流的输出类型
class SplitTempProcessor(threshold: Double) extends ProcessFunction[SensorReading, SensorReading] {
  override def processElement(i: SensorReading, context: ProcessFunction[SensorReading, SensorReading]#Context, collector: Collector[SensorReading]): Unit = {
    //如果温度大于threshold，输出到主流，否则到测输出流
    if (i.temp > threshold) {
      collector.collect(i)
    } else {
      //测输出流使用context.output进行输出
      context.output(new OutputTag[(Long, String, Double)]("low"), (i.id, i.sensorName, i.temp))
    }
  }
}
