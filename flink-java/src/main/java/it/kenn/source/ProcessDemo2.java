package it.kenn.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import scala.Tuple2;
import scala.Tuple3;

import java.time.Duration;

/**
 * process function学习
 * 2020-12-19
 */
public class ProcessDemo2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        SingleOutputStreamOperator<Tuple3<String, Long, Double>> source = env.addSource(new ForJoinSource1())
                //指定watermark生成策略
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple3<String, Long, Double>>forBoundedOutOfOrderness(Duration.ofMillis(100))
                        .withTimestampAssigner((e, ts) -> e._2())
                );
        source.keyBy(e -> e._1())
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

/**
 * 下面方法完成的功能是：
 * 如果某个键对应的值6s没有被修改就会输出这个键对应的计数。这个功能其实可以使用session window来实现
 */
class CountWithTimeoutFunction extends KeyedProcessFunction<String, Tuple3<String, Long, Double>, Tuple2<String, Long>> {
    private ValueState<CountWithTimestamp> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
    }

    @Override
    public void processElement(Tuple3<String, Long, Double> value, Context context, Collector<Tuple2<String, Long>> collector) throws Exception {
        CountWithTimestamp current = state.value();
        if (current == null) {
            current = new CountWithTimestamp();
            current.key = value._1();
        }
        current.count++;
        current.lastModified = value._2();
        state.update(current);
        /**
         * 注册定时器。对每个[键,时间戳]的组合仅仅会注册一个定时器。如果[键,时间戳]注册了多个定时器，会被"去重",即注册的多个重复定时器仅会被调用一次
         * 由于每个[键,时间戳]仅仅会有一个定时器，我们可以在注册定时器的时候将时间戳到精度调小一点来减少定时器的数量。比如我们设计时间戳的精度为1s，那么理论上
         * 最多每秒才会产生一个定时器。
         *
         * 设置时间戳的精度为1s可以将代码改为：
         * context.timerService().registerEventTimeTimer( ((current.lastModified / 1000) * 1000) + 6000)
         */
        context.timerService().registerEventTimeTimer(current.lastModified + 6000);

        //停掉一个定时器，参数为需要停掉定时器的时间
//        long timestampOfTimerToStop = ...
//        context.timerService().deleteEventTimeTimer(timestampOfTimerToStop);
    }

    /**
     * 回调函数
     *
     * @param timestamp
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out) throws Exception {
        CountWithTimestamp result = state.value();
        if (timestamp == result.lastModified + 6000) {
            out.collect(new Tuple2<>(result.key, result.count));
        }
    }
}
