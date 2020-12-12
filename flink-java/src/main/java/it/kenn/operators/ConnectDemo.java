package it.kenn.operators;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @author yulei
 * @date 2020-11-29
 * 测试connect的使用
 */
public class ConnectDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> intStream = env.fromElements(1, 2, 3, 4, 5, 6, 7);
        DataStreamSource<String> strStream = env.fromElements("aa", "bb", "cc", "dd", "ee");
        ConnectedStreams<Integer, String> connectedStreams = intStream.connect(strStream);
        SingleOutputStreamOperator<Tuple2<String, Integer>> useCoMapFuncConnectedStream = connectedStreams.map(new CoMapFunction<Integer, String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map1(Integer e) throws Exception {
                return new Tuple2<>("default", e);
            }

            @Override
            public Tuple2<String, Integer> map2(String s) throws Exception {
                return new Tuple2<>(s, 0);
            }
        });
        useCoMapFuncConnectedStream.print();
        env.execute();
    }
}
