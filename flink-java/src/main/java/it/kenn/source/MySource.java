package it.kenn.source;

import it.kenn.entities.User;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

/**
 * @author yulei
 * 做一个员工数据源，用于生成员工数据
 */
public class MySource implements SourceFunction<User> {

    public void run(SourceContext sourceContext) {
        Random random = new Random();
        while (true) {
            int i = random.nextInt(10000);
            int f1 = random.nextInt(30);
            int f2 = random.nextInt(30);
            int f3 = random.nextInt(30);
            double d = random.nextDouble();
            sourceContext.collect(
                    new User(i, "user" + i, d, new ArrayList<>(Arrays.asList("user" + f1, "user" + f2, "user" + f3))));
        }
    }

    public void cancel() {
    }
}
