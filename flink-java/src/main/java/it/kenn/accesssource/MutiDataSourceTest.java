package it.kenn.accesssource;

import it.kenn.demo.HbaseUser;
import it.kenn.source.HBaseSource;
import it.kenn.source.MySQLSource;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.time.Duration;

/**
 * 多数据与接入测试
 */
public class MutiDataSourceTest {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //指定事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.readTextFile("/Users/yulei/IdeaProjects/personal/LearningFlink/flink-java/flink-java.iml").print("text source");
        env.socketTextStream("localhost",9000).print("socket source");
        //MySQL数据读取
        env.addSource(new MySQLSource()).print("mysql source");
        //hbase数据源
        env.addSource(new HBaseSource()).print("hbase source");
        //hdfs数据源
        env.readTextFile("hdfs://localhost:8020/user/bilibili").print("hdfs source");



        env.execute();
    }
}
