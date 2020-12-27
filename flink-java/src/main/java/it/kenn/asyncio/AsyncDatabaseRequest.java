package it.kenn.asyncio;

import it.kenn.pojo.Click;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;


/**
 * @date 2020-12-27
 * 异步io测试
 * 参考文档：https://www.cnblogs.com/zz-ksw/p/13228642.html
 */
public class AsyncDatabaseRequest extends RichAsyncFunction<Click, Tuple5<String, Integer,String, String,String>> {

    private transient Connection client;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        client = DriverManager.getConnection("url", "root", "root");
        client.setAutoCommit(false);
    }

    @Override
    public void close() throws Exception {
        client.close();
    }

    @Override
    public void asyncInvoke(Click input, ResultFuture<Tuple5<String, Integer,String, String,String>> resultFuture) throws Exception {
        List<Tuple5<String, Integer, String, String, String>> list = new ArrayList<>();
        Statement statement = client.createStatement();
        ResultSet resultSet = statement.executeQuery("select user_name, age, gender from user_data_for_join where user_name=" + input);

        if (resultSet != null && resultSet.next()) {
            String name = resultSet.getString("user_name");
            int age = resultSet.getInt("age");
            String gender = resultSet.getString("gender");
            Tuple5<String, Integer,String, String,String> res = Tuple5.of(name, age, gender, input.getSite(), input.getTime());
            list.add(res);
        }

        // 将数据搜集
        resultFuture.complete(list);
    }

    @Override
    public void timeout(Click input, ResultFuture<Tuple5<String, Integer,String, String,String>> resultFuture) throws Exception {

    }
}








