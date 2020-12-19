package it.kenn.demo;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import static org.apache.flink.table.api.Expressions.*;
import org.apache.flink.types.Row;


/**
 * flink table 示例
 */
public class TableDemo {
    //创建表的时候需要注意，表名是区分大小写的！比如下面的KafkaTable写出小写的kafkatable就会报table not found
    private static String kafkaTable = "CREATE TABLE KafkaTable (\n" +
            "  `user` STRING,\n" +
            "  `site` STRING,\n" +
            "  `time` STRING\n" +
            ") WITH (\n" +
            "  'connector' = 'kafka',\n" +
            "  'topic' = 'test-old',\n" +
            "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
            "  'properties.group.id' = 'testGroup',\n" +
            "  'scan.startup.mode' = 'earliest-offset',\n" +
            "  'format' = 'json'\n" +
            ")";
    //注意，为了防止表的字段是关键字，最好把表字段都用反引号引起来
    private static String esTable = "CREATE TABLE esTable (\n" +
            "  `user` STRING,\n" +
            "  `site` STRING,\n" +
            "  `time` STRING,\n" +
            "  PRIMARY KEY (`time`) NOT ENFORCED\n" +
            ") WITH (\n" +
            "  'connector' = 'elasticsearch-7',\n" +
            "  'hosts' = 'http://localhost:9200',\n" +
            "  'index' = 'flink_kafka_users'\n" +
            ")";

    public static void main(String[] args) throws Exception {
        //环境设置
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建table environment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, environmentSettings);
        //注册表
        tableEnv.executeSql(kafkaTable);
        tableEnv.executeSql(esTable);
        //打印表结构
        tableEnv.from("KafkaTable").printSchema();
        //查询表
        Table table = tableEnv.from("KafkaTable").select($("user"),$("site"),$("time"));
        //将表的数据插入到es中
        //这里的sql中样需要注意字段是关键字的可能性
        tableEnv.executeSql("insert into esTable select `user`, site, `time` from KafkaTable");
        //将表的数据转为DataStream并输出到控制台
        tableEnv.toAppendStream(table, Row.class).print("site:");
        env.execute();
    }
}
