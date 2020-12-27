package it.kenn.demo;

import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;

public class CatalogDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Catalog mysqlCatalog = new JdbcCatalog("default", "aspirin", "root", "root", "jdbc:mysql://localhost:3306/aspirin");
        tableEnv.registerCatalog("mysqlCatalog", mysqlCatalog);
        tableEnv.useCatalog("mysqlCatalog");
        String[] databases = tableEnv.listDatabases();
        System.out.println(databases);
        env.execute();
    }
}
