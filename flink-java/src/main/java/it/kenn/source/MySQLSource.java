package it.kenn.source;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import it.kenn.demo.LoginUser;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class MySQLSource extends RichSourceFunction<LoginUser> {
    private Connection connection = null;
    private PreparedStatement ps = null;
    private volatile boolean isRunning = true;

    public void run(SourceContext sourceContext) throws Exception {
        ResultSet resultSet = ps.executeQuery();
        while (isRunning && resultSet.next()){
            LoginUser user = new LoginUser();
            user.setId(resultSet.getLong("id"));
            user.setUsername(resultSet.getString("username"));
            user.setPassword(resultSet.getString("password"));
            user.setRole(resultSet.getString("role"));
            sourceContext.collect(user);
        }

    }

    public void cancel() {
        try {
            super.close();
            if (connection != null) {
                connection.close();
            }
            if (ps != null) {
                ps.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        isRunning = false;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource.setUrl("jdbc:mysql://localhost:3306/aspirin?useSSL=false");
        dataSource.setUsername("root");
        dataSource.setPassword("root");

        connection = dataSource.getConnection();
        ps = connection.prepareStatement("select id, username, password, role from user");

    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
