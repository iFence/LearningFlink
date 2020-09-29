package it.aspirin.datastreamAPI

import it.aspirin.utils.FlinkUtils
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.DataStream

case class SensorReading(id:String, timestamp:Long, temperature: Double)

object HelloFlink {
  def main(args: Array[String]): Unit = {
    val env = FlinkUtils.getStreamEnv
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    FlinkUtils.start(env)
  }

  def avgSensorReading(): DataStream[String] ={

    null
  }
}


