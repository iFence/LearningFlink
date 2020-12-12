package it.kenn.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import org.apache.flink.table.planner.factories.TestValuesTableFactory;

/**
 * Description: Flink-CDC 测试
 * Date: 2020/11/16 20:52
 *
 * @author kenn
 */
public class FlinkCDCSQLTest {

    public static void main(String[] args) throws Exception {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, fsSettings);

        // 数据源表
        String sourceDDL =
                "CREATE TABLE mysql_binlog (\n" +
                        " id INT NOT NULL,\n" +
                        " name STRING,\n" +
                        " description STRING\n" +
                        ") WITH (\n" +
                        " 'connector' = 'mysql',\n" +
                        " 'hostname' = 'localhost',\n" +
                        " 'port' = '3306',\n" +
                        " 'username' = 'root',\n" +
                        " 'password' = 'root',\n" +
                        " 'database-name' = 'aspirin',\n" +
                        " 'table-name' = 'tb_products_cdc'\n" +
                        ")";
        // 输出目标表
        String sinkDDL =
                "CREATE TABLE tb_sink (\n" +
                        " name STRING,\n" +
                        " countSum BIGINT,\n" +
                        " PRIMARY KEY (name) NOT ENFORCED\n" +
                        ") WITH (\n" +
                        " 'connector' = 'print'\n" +
                        ")";
        // 简单的聚合处理
        String transformSQL =
                "INSERT INTO tb_sink " +
                        "SELECT name, COUNT(1) " +
                        "FROM mysql_binlog " +
                        "GROUP BY name";

        tableEnv.executeSql(sourceDDL);
        tableEnv.executeSql(sinkDDL);
        TableResult result = tableEnv.executeSql(transformSQL);

        // 等待flink-cdc完成快照
//        waitForSnapshotStarted("tb_sink");
        result.print();

        result.getJobClient().get().cancel().get();
    }

//    private static void waitForSnapshotStarted(String sinkName) throws InterruptedException {
//        while (sinkSize(sinkName) == 0) {
//            Thread.sleep(100);
//        }
//    }
//
//    private static int sinkSize(String sinkName) {
//        synchronized (TestValuesTableFactory.class) {
//            try {
//                return TestValuesTableFactory.getRawResults(sinkName).size();
//            } catch (IllegalArgumentException e) {
//                // job is not started yet
//                return 0;
//            }
//        }
//    }

}

