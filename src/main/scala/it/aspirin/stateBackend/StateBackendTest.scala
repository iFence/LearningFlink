package it.aspirin.stateBackend

import java.util.concurrent.TimeUnit

import it.aspirin.utils.FlinkUtils
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @author yulei
 */
object StateBackendTest {
  def main(args: Array[String]): Unit = {
    val env = FlinkUtils.getStreamEnv
    //内存后端，默认就是这个
    env.setStateBackend(new MemoryStateBackend())
    //文件系统状态后端
    env.setStateBackend(new FsStateBackend("htfs://localhost:50070/flink/checkpoint"))
    //rocksDb 状态需要导包
//    env.setStateBackend(new RocksDbStateBackend())
    FlinkUtils.start(env, "state backend test")

    //在Flink代码中默认是不启用checkpoint的，必须先启用checkpoint
    env.enableCheckpointing(1000) //单位毫秒，一秒产生一个checkpoint，也就是第一个checkpoint产生以后，再过1s立即产生下一个checkpoint
    env.enableCheckpointing(200, CheckpointingMode.EXACTLY_ONCE) //精确一次的检查点
    env.getCheckpointConfig.setCheckpointTimeout(1000L) //checkpoint从产生，到告诉JM我已经完成了这个时间点的间隔就是timeout，如果太长时间这个checkpoint都没有完成，那这个checkpoint就放弃了
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2) //允许同时出现几个checkpoint处理，默认一个。什么意思呢？每隔1s会触发产生一个checkpoint，但是如果目前这个checkpoint还没有保存，那么默认情况下，即使过去1s中了，仍然不会产生新的checkpoint，直到当前checkpoint提交了
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000L) //两个checkpoint之间的最小间隔时间，这个跟第一个参数有所区别，即指定的是第一个checkpoint结束以后，距离下一个checkpoint产生之前的间隔时间，这个参数设置以后，会把上面的setMaxConcurrentCheckpoints参数覆盖掉
    env.getCheckpointConfig.setPreferCheckpointForRecovery(true) //相比savepoint，是否更倾向于使用savepoint做故障恢复，默认false
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3) //容忍多少次checkpoint失败，默认值为0

    //重启策略
    env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.of(5, TimeUnit.MINUTES), Time.of(5, TimeUnit.MINUTES)))
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000 * 10)) //重启三次,间隔10s


  }

  /**
   *
   */
  def addSource(env: StreamExecutionEnvironment): Unit = {
    env.socketTextStream("localhost", 1111)
  }
}
