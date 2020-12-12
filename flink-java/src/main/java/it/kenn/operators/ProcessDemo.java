package it.kenn.operators;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import scala.Tuple2;

/**
 * 测试process函数的使用
 * events (stream elements)
 * state (fault-tolerant, consistent, only on keyed stream)
 * timers (event time and processing time, only on keyed stream)
 * <p>
 * ProcessFunction可以被认为是一个可访问键控状态和计时器的FlatMapFunction。它通过为输入流中接收到的每个事件调用来处理事件。
 */
public class ProcessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<Tuple2<String, String>> readyStream = source.map(n -> {
            String[] strings = n.split(",");
            return new Tuple2<>(strings[0], strings[1]);
        }).returns(TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
        }));
        readyStream.keyBy(n -> n._1)
                .process(new CountWithTimeoutFunction())
                .print();

        env.execute();
    }
}

class CountWithTimestamp {

    public String key;
    public long count;
    public long lastModified;
}

class CountWithTimeoutFunction extends KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<String, Long>> {

    /**
     * The state that is maintained by this process function
     */
    private ValueState<CountWithTimestamp> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
    }

    @Override
    public void processElement(Tuple2<String, String> value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {

        // retrieve the current count
        CountWithTimestamp current = state.value();
        if (current == null) {
            current = new CountWithTimestamp();
            current.key = value._1;
        }

        // update the state's count
        current.count++;
        // set the state's timestamp to the record's assigned event time timestamp
        //由于socket流中没有带时间戳所以这里是空的
//        current.lastModified = ctx.timestamp();
        current.lastModified = System.currentTimeMillis();

        // write the state back
        state.update(current);

        // schedule the next timer 60 seconds from the current event time
        ctx.timerService().registerEventTimeTimer(current.lastModified + 10000);
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<Tuple2<String, Long>> out) throws Exception {
        CountWithTimestamp result = state.value();

        // check if this is an outdated timer or the latest timer
        System.out.println("ts: "+timestamp + result.lastModified+ 10000 + ".dd.");
        if (timestamp == result.lastModified + 10000) {
            // emit the state on timeout
            out.collect(new Tuple2<String, Long>(result.key, result.count));
        }
    }
}
