package it.kenn.acc;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 累加器测试
 */
public class AccumulatorTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        AccumulatorTest accumulatorTest = new AccumulatorTest();
        accumulatorTest.process(env);

    }

    public void process(StreamExecutionEnvironment env) throws Exception {

        SingleOutputStreamOperator<String> registerAccStream = env
                .readTextFile("/Users/yulei/IdeaProjects/personal/LearningFlink/flink-java/src/main/resources/hbase-site.xml")
                .map(new RichMapFunction<String, String>() {
                    //1.创建累加器
                    private IntCounter numLines = new IntCounter();

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //2.注册累加器
                        getRuntimeContext().addAccumulator("num-lines", numLines);
                    }

                    @Override
                    public String map(String s) throws Exception {
                        //3.使用累加器
                        this.numLines.add(1);
                        return s;
                    }
                });
        registerAccStream.print();
        //4. 得到累加器结果，注意结果是从execute方法的返回值中得到的
        JobExecutionResult result = env.execute();
        Object accRes = result.getAccumulatorResult("num-lines");
        System.out.println("文件共："+accRes + "行。");
    }
}
