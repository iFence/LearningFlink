package it.kenn.demo;

import it.kenn.pojo.Click;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.*;

/**
 * 流表join维表
 */
public class JoinDemo {
    private static String dimTable = "CREATE TABLE dimTable (\n" +
            "  id int,\n" +
            "  user_name STRING,\n" +
            "  age INT,\n" +
            "  gender STRING,\n" +
            "  PRIMARY KEY (id) NOT ENFORCED\n" +
            ") WITH (\n" +
            "   'connector'='jdbc',\n" +
            "   'username'='root',\n" +
            "   'password'='root',\n" +
            "   'url'='jdbc:mysql://localhost:3306/aspirin',\n" +
            "   'table-name'='user_data_for_join'\n" +
            ")";

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

    private static String wideTable = "CREATE TABLE wideTable (\n" +
            "  id int,\n" +
            "  site STRING,\n" +
            "  user_name STRING,\n" +
            "  age INT,\n" +
            "  ts STRING,\n" +
            "  PRIMARY KEY (id) NOT ENFORCED\n" +
            ") WITH (\n" +
            "   'connector'='jdbc',\n" +
            "   'username'='root',\n" +
            "   'password'='root',\n" +
            "   'url'='jdbc:mysql://localhost:3306/aspirin',\n" +
            "   'table-name'='wide_table'\n" +
            ")";
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
        tableEnvironment.executeSql(dimTable);
        tableEnvironment.executeSql(kafkaTable);
        tableEnvironment.executeSql(wideTable);
        Table mysqlTable = tableEnvironment.from("dimTable").select("id, user_name, age, gender");
        Table kafkaTable = tableEnvironment.from("KafkaTable").select($("user"), $("site"), $("time"));

        DataStream<Click> clickDataStream = tableEnvironment.toAppendStream(kafkaTable, Click.class);

        String joinSql = "insert into wideTable " +
                " select " +
                "   dimTable.id as `id`, " +
                "   t.site as site, " +
                "   dimTable.user_name as user_name, " +
                "   dimTable.age as age, " +
                "   t.`time` as ts " +
                "from KafkaTable as t " +
                "left join dimTable on dimTable.user_name = t.`user`";
//        tableEnvironment.executeSql(joinSql);

        env.execute();
    }
}

