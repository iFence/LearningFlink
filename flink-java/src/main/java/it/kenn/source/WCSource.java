package it.kenn.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import scala.Tuple2;

import java.util.Random;

/**
 * word count source
 */
public class WCSource implements SourceFunction<Tuple2<String, String>> {
    private boolean flag = true;

    @Override
    public void run(SourceContext<Tuple2<String, String>> sourceContext) throws Exception {
        Random random = new Random();
        while (flag) {
            int i = random.nextInt();
            sourceContext.collect(new Tuple2<>("ddd" + i, i + "adssa"));
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}
