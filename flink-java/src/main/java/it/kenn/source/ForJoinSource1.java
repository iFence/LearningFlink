package it.kenn.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import scala.Tuple3;

import java.util.Random;

public class ForJoinSource1 implements SourceFunction<Tuple3<String, Long, Double>> {
    boolean flag = true;

    @Override
    public void run(SourceContext<Tuple3<String, Long, Double>> ctx) throws Exception {
        Random random = new Random();
        while (flag) {
            int randInt = random.nextInt(100);
            ctx.collect(new Tuple3<>("S" + randInt, System.currentTimeMillis(), random.nextDouble() * 1000));
            Thread.sleep(30);
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}

