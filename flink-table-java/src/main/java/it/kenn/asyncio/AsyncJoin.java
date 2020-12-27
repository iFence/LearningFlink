package it.kenn.asyncio;

import it.kenn.pojo.Click;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 利用异步io实现事实表与维表的join操作
 */
public class AsyncJoin {

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

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
        //注册表
        tableEnvironment.executeSql(kafkaTable);
        Table kafkaTable = tableEnvironment.from("KafkaTable").select($("user"), $("site"), $("time"));
        //将
        DataStream<Click> clickDataStream = tableEnvironment.toAppendStream(kafkaTable, Click.class);
        SingleOutputStreamOperator<Tuple5<String, Integer, String, String, String>> asyncStream =
                //注意这里将事实表和维度表连接起来的是AsyncDataStream.unorderedWait方法
                AsyncDataStream.unorderedWait(clickDataStream, new AsyncDatabaseRequest(), 1000, TimeUnit.MILLISECONDS, 100);

        asyncStream.print("async:");
        env.execute("AsyncIOFunctionTest");
    }
}



