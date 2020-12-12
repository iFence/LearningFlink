package it.kenn.window;


import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


/**
 * flink窗口算子测试
 */
public class WindowDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple3<String, Long, Double>> readyStream = env.socketTextStream("localhost", 9999)
                .map(event -> {
                    String[] strings = event.split(",");
                    return new Tuple3<>(strings[0], Long.parseLong(strings[1]), Double.parseDouble(strings[2]));
                })
                //这里指定返回值类型
                .returns(TypeInformation.of(new TypeHint<Tuple3<String, Long, Double>>() {
                }));

        //----------------------reduce function------------------------
        readyStream.keyBy(event -> event.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<Tuple3<String, Long, Double>>() {
                    @Override
                    public Tuple3<String, Long, Double> reduce(Tuple3<String, Long, Double> v1, Tuple3<String, Long, Double> v2) throws Exception {
                        return new Tuple3<>(v2.f0, v2.f1, v1.f2 + v2.f2);
                    }
                }).print("reduce:");

        //----------------------process function------------------------
        readyStream.keyBy(event -> event.f0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new AggregateFunction<Tuple3<String, Long, Double>, Tuple2<Long, Double>, Tuple2<String, Double>>() {
                    //创建一个累加器
                    @Override
                    public Tuple2<Long, Double> createAccumulator() {
                        return new Tuple2<>(0L, 0.0D);
                    }

                    //两个参数一个是输入的元素，一个是累加器，我们要做的是将输入的元素合并到累加器当中
                    @Override
                    public Tuple2<Long, Double> add(Tuple3<String, Long, Double> value, Tuple2<Long, Double> acc) {
                        return new Tuple2<>(acc.f0 + 1, acc.f1 + value.f2);
                    }

                    //两个参数是两个累加器，将两个累加器中的内容进行合并然后输出
                    @Override
                    public Tuple2<Long, Double> merge(Tuple2<Long, Double> acc1, Tuple2<Long, Double> acc2) {
                        return new Tuple2<>(acc1.f0 + acc2.f0, acc1.f1 + acc1.f1);
                    }

                    //将输出最终结果。这里的参数midRes就是上面合并的结果，根据需要可以最这个结果直接输出或者再做一些其他操作，比如下面求了平均值
                    @Override
                    public Tuple2<String, Double> getResult(Tuple2<Long, Double> midRes) {
                        return new Tuple2<>(midRes.f0 + "", midRes.f1 / midRes.f0);
                    }
                }).print("agg:");

        //----------------------ProcessWindowFunction------------------------
        readyStream.keyBy(event -> event.f0)
                .timeWindow(Time.seconds(5))
                .process(new MyProcessWindowFunction())
                .print("process:");
        //一条输出：process::12> Window: TimeWindow{start=1607433915000, end=1607433920000}count: 16

        //----------------------增量聚合函数: reduce&process:----------------------
        //下面传入了两个函数，一个reduce，一个process，reduce负责筛选出窗口中最小的值，process负责输出（如果有其他操作当然也可以做），像下面这种情况就相当于把process去掉的情况，因为没有process，只有reduce函数也能完成同样的工作
        readyStream.keyBy(e -> e.f0)
                .timeWindow(Time.seconds(5))
                .reduce(new ReduceFunction<Tuple3<String, Long, Double>>() {
                    @Override
                    public Tuple3<String, Long, Double> reduce(Tuple3<String, Long, Double> v1, Tuple3<String, Long, Double> v2) throws Exception {
                        return v1.f2 > v2.f2 ? v2 : v1;
                    }
                }, new ProcessWindowFunction<Tuple3<String, Long, Double>, Tuple3<String, Long, Double>, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple3<String, Long, Double>> iterable, Collector<Tuple3<String, Long, Double>> collector) throws Exception {
                        Tuple3<String, Long, Double> min = iterable.iterator().next();
                        collector.collect(min);
                    }
                }).print("reduce&process:");

        //----------------------增量聚合函数: reduce&process:----------------------
        env.execute();
    }
}

//四个泛型分别是输入参数，输出参数，Key和TimeWindow，TimeWindow是窗口的开窗时间和关窗时间
class MyProcessWindowFunction extends ProcessWindowFunction<Tuple3<String, Long, Double>, String, String, TimeWindow> {

    //这个函数的唯一功能就是记录了这个窗口中共进来了多少数据
    @Override
    public void process(String key, Context context, Iterable<Tuple3<String, Long, Double>> input, Collector<String> out) {
        long count = 0;
        for (Tuple3<String, Long, Double> in : input) {
            count++;
        }
        out.collect("Window: " + context.window() + "count: " + count);
    }
}
