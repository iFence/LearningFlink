package it.kenn.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SocketSource {
    public static DataStream<String> getSocketSource(StreamExecutionEnvironment env, String host, Integer port) {
        return env.socketTextStream(host, port);
    }
}
