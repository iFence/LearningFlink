package it.kenn.state;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyedStateDemo {
    public static void main(String[] args) throws Exception {
        String backendPath = "hdfs://localhost:8020/flink/statebackend/fsState";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(6);
        //设置状态后端为hdfs
        StateBackend stateBackend = new FsStateBackend(backendPath);
        env.setStateBackend(stateBackend);
        env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 4L), Tuple2.of(1L, 2L))
                //进行keyBy操作
                .keyBy(value -> value.f0)
                //有状态的flatMap
                .flatMap(new CountWindowAverage())
                //简单的输出
                .print();
        env.execute();
    }
}


