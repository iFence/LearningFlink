package it.aspirin.stateManagement

import java.util

import it.aspirin.stateManagement.SMTest.SensorReading
import it.aspirin.utils.FlinkUtils
import org.apache.flink.api.common.functions.{ReduceFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ReducingState, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * 状态编程 测试
 */
object SMTest {

  case class SensorReading(name: String, id: Long, temp: Double)

  def main(args: Array[String]): Unit = {
    val env = FlinkUtils.getStreamEnv
    val sourceStream = addSocketSource(env)
    val sinkStream = addTrans(sourceStream)
    addConsoleSink(sinkStream)
    FlinkUtils.start(env, "state management test")
  }

  def addSocketSource(env: StreamExecutionEnvironment): DataStream[String] = {
    env.socketTextStream("localhost", 7777)
  }

  def addTrans(sourceStream: DataStream[String]) = {
    sourceStream.map(line => {
      val arr = line.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })
  }

  def addConsoleSink(sinkStream: DataStream[SensorReading]): Unit = {
    sinkStream.print("my sink")
  }
}

/**
 * 键控状态的使用
 */
class MyRichMapper extends RichMapFunction[SensorReading, String] {
  //值状态
  var valueState: ValueState[Double] = _
  lazy val valueState1: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("valueState1", classOf[Double]))

  //列表状态
  lazy val listState: ListState[Int] = getRuntimeContext.getListState(new ListStateDescriptor[Int]("list State", classOf[Int]))

  //map状态
  lazy val mapState: MapState[String, Double] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Double]("map state", classOf[String], classOf[Double]))

  //reduce state
  //三个参数，第一个未该值取一个名字，第三个序列化，第二个传入一个reducer函数
  lazy val reduceState: ReducingState[SensorReading] = getRuntimeContext.getReducingState(new ReducingStateDescriptor[SensorReading]("", new MyReducer, classOf[SensorReading]))

  override def open(parameters: Configuration): Unit = {
    // 下面这句话需要定义在open生命周期里面,或者把这句话定义成lazy的
    valueState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("valueState1", classOf[Double]))
  }

  override def map(in: SensorReading): String = {
    valueState.value() //获取状态
    valueState.update(valueState.value() + 2) //更新状态

    val list = new util.ArrayList[Int]()
    list.add(2)
    list.add(3)
    //将值追加进去
    listState.addAll(list)
    //将值更新进去，即之前的值不要了，直接用传入的数组中的值
    listState.update(list)
    //获取listState中的值
    listState.get()

    if (mapState.contains("map state")) {
      mapState.get("map state")
      mapState.put("hahaha", 23.3)
    }

    reduceState.get() //获取

    reduceState.add(SensorReading("test", 1L, 12.34)) //将添加的这个值，根据传入的reduce函数聚合起来
    ""
  }
}

//取两个温度中较小的那个值
class MyReducer extends ReduceFunction[SensorReading] {
  override def reduce(t: SensorReading, t1: SensorReading): SensorReading = {
    SensorReading(t.name, t.id, t.temp.min(t1.temp))
  }
}
