package it.kenn.state;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;


public class BroadcastStreamDemo2 {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final MapStateDescriptor<String, String> CONFIG_KEYWORDS = new MapStateDescriptor<>(
                "config-keywords",
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO);

        BroadcastStream<String> broadcastStream = env.addSource(new Test0Source()).broadcast(CONFIG_KEYWORDS);

        DataStream<String> dataStream = env.addSource(new TestSource());

        // 数据流和广播流连接处理并将拦截结果打印
        dataStream.connect(broadcastStream).process(new BroadcastProcessFunction<String, String, String>() {

            //拦截的关键字
            private String keywords = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                keywords = "java";
                System.out.println("初始化模拟连接数据库读取拦截关键字：java");
            }

            @Override
            public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                if (value.contains(keywords)) {
                    out.collect("拦截消息:" + value + ", 原因:包含拦截关键字：" + keywords);
                }
            }

            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
                keywords = value;
                System.out.println("关键字更新成功，更新拦截关键字：" + value);
            }
        }).print();

        env.execute();
    }
}

class Test0Source extends RichSourceFunction<String> {

    private volatile boolean isRunning = true;
    //测试数据集
    private String[] dataSet = new String[]{
            "java",
            "swift",
            "php",
            "go",
            "python"
    };

    /**
     * 数据源：模拟每30秒随机更新一次拦截的关键字
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        int size = dataSet.length;
        while (isRunning) {
            TimeUnit.SECONDS.sleep(30);
            int seed = (int) (Math.random() * size);
            //随机选择关键字发送
            ctx.collect(dataSet[seed]);
            System.out.println("读取到上游发送的关键字:" + dataSet[seed]);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}

class TestSource extends RichSourceFunction<String> {

    private volatile boolean isRunning = true;

    //测试数据集
    private String[] dataSet = new String[]{
            "java是世界上最优秀的语言",
            "swift是世界上最优秀的语言",
            "php是世界上最优秀的语言",
            "go是世界上最优秀的语言",
            "python是世界上最优秀的语言"
    };

    /**
     * 模拟每3秒随机产生1条消息
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        int size = dataSet.length;
        while (isRunning) {
            TimeUnit.SECONDS.sleep(3);
            int seed = (int) (Math.random() * size);
            ctx.collect(dataSet[seed]);
            System.out.println("读取到上游发送的消息：" + dataSet[seed]);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

}
