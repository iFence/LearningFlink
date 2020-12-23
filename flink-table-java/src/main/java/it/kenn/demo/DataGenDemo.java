package it.kenn.demo;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 测试datagen数据源和print sink的使用
 * datagen可以创建无界表也可以创建有界表
 * <p>
 * print sink方式支持的数据类型很有限，DECIMAL类型都不支持，复合结构更不支持。
 */
public class DataGenDemo {
    static String gen = "CREATE TABLE Orders (\n" +
            "    order_number BIGINT,\n" +
            "    price        INT,\n" +
            "    order_time   String\n" +
            ") WITH (\n" +
            "  'connector' = 'datagen'\n" +
            ")";

    /**
     * 下面是创建一张有界表，而且参照物理表的表结构创建的。
     */
    static String bound_gen = "CREATE TEMPORARY TABLE GenOrders\n" +
            "WITH (\n" +
            "    'connector' = 'datagen',\n" +
            "    'number-of-rows' = '10'\n" +
            ")\n" +
            "LIKE Orders (EXCLUDING ALL)";

    /**
     * 按照上面Orders表的表结构创建一张新表
     */
    static String print = "CREATE TABLE print_table WITH ('connector' = 'print')\n" +
            "LIKE Orders (EXCLUDING ALL)";

    public static void main(String[] args) throws Exception {
        //环境设置
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建table environment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, environmentSettings);

        tableEnv.executeSql(gen);
        tableEnv.executeSql(print);

        String transSQL = "insert into print_table select * from Orders";
        tableEnv.executeSql(transSQL);
        env.execute();
    }
}
