package it.kenn.demo;

import it.kenn.entities.User;
import it.kenn.source.MySource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class ProcessFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<User> source = env.addSource(new MySource());
        //注意后面的大括号要有
        final OutputTag<User> genStaff = new OutputTag<User>("genStaff") {};
        SingleOutputStreamOperator<User> splitStream = source
                //将员工号小于100的员工单独过滤出来放到主流当中（认为前100名员工是高管）
                .process(new ProcessFunction<User, User>() {
                    public void processElement(User user, Context context, Collector<User> collector) throws Exception {
                        if (user.getId() < 100) collector.collect(user);
                        else context.output(genStaff, user);
                    }
                });
        DataStream<User> genStream = splitStream.getSideOutput(genStaff);


        env.execute("hello world");
    }
}
