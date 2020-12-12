package it.kenn.statebackend;

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

public class StateBackendDemo {
    private static final String stateBackendUri = "hdfs://localhost:8020/flink/statebackend";
    public static void main(String[] args) throws IOException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10);
        MemoryStateBackend memoryStateBackend = new MemoryStateBackend();
        FsStateBackend fsStateBackend = new FsStateBackend(stateBackendUri);
        StateBackend rocksDBStateBackend1 = new RocksDBStateBackend(stateBackendUri);
        StateBackend rocksDBStateBackend2 = new RocksDBStateBackend(fsStateBackend);
        env.setStateBackend(fsStateBackend);
    }
}
