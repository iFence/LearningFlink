package it.kenn.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * 下面是一个ValueState的例子
 */
//需要注意的是要使用RichFlatMapFunction，因为Rich抽象类里面有生命周期函数，比如下面要用到的open
public class CountWindowAverage extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
    private transient ValueState<Tuple2<Long, Long>> sum;

    @Override
    public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> collector) throws Exception {
        //这里编写的是真正的业务代码，我们要做的是对传入的值进行累加，并记录有多少个值进行了累加
        Tuple2<Long, Long> currValue = sum.value();
        currValue.f0 += 1;
        currValue.f1 += input.f1;
        //更新状态
        sum.update(currValue);
        //计算平均值
        if (currValue.f0 >= 2) {
            collector.collect(new Tuple2<>(input.f0, currValue.f1 / currValue.f0));
            sum.clear();
        }
    }

    /**
     * 在open函数里面可以使用getRuntimeContext函数来getState,但在此之前需要定义ValueStateDescriptor
     * ValueStateDescriptor有三个参数，第一个为该state起名字，第二个定义该state的类型，第三个初始化值
     * @param parameters
     */
    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Tuple2<Long, Long>> descriptor = new ValueStateDescriptor<>(
                "average",
                TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {})
                ,new Tuple2<>(0L,0L)
        );
        //第一次调用的时候完成对ValueState初始化并得到初始值
        sum = getRuntimeContext().getState(descriptor);

    }
}
