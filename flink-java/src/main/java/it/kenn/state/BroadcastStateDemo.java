package it.kenn.state;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * 广播流与广播状态
 */
public class BroadcastStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Item> itemStream = env.addSource(new ItemsSource());
        KeyedStream<Item, Color> colorPartitionedStream = itemStream.keyBy(new KeySelector<Item, Color>() {
            @Override
            public Color getKey(Item item) throws Exception {
                return item.getColor();
            }
        });
        DataStreamSource<Rule> ruleStream = env.addSource(new RuleSource());
        //定义MapState，将来将其广播出去
        MapStateDescriptor<String, Rule> ruleStateDescriptor = new MapStateDescriptor<>(
                "RulesBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<Rule>() {
                }));
        //这里是将ruleStream广播出去，并将MapState传入ruleStream中
        BroadcastStream<Rule> ruleBroadcastStream = ruleStream.broadcast(ruleStateDescriptor);

        SingleOutputStreamOperator<String> resStream = colorPartitionedStream
                //连接两个流
                .connect(ruleBroadcastStream)
                //四个泛型分别是key，in1，in2，out，可以看源码能看出来
                .process(new KeyedBroadcastProcessFunction<Color, Item, Rule, String>() {

                    // 再定义一个MapState，存储规则中 的第一个元素并等待第二个元素的到来
                    //todo 这里为什么存了一个列表还是没有太明白
                    //we keep a list as we may have many first elements waiting
                    //想明白了。因为rule也是一个流
                    private final MapStateDescriptor<String, List<Item>> mapStateDesc =
                            new MapStateDescriptor<>(
                                    "items",
                                    BasicTypeInfo.STRING_TYPE_INFO,
                                    new ListTypeInfo<>(Item.class));

                    // 和上面定义的ruleStateDescriptor一模一样
                    private final MapStateDescriptor<String, Rule> ruleStateDescriptor =
                            new MapStateDescriptor<>(
                                    "RulesBroadcastState",
                                    BasicTypeInfo.STRING_TYPE_INFO,
                                    TypeInformation.of(new TypeHint<Rule>() {}));

                    @Override
                    public void processElement(Item value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        final MapState<String, List<Item>> state = getRuntimeContext().getMapState(mapStateDesc);
                        final Shape shape = value.getShape();

                        for (Map.Entry<String, Rule> entry : ctx.getBroadcastState(ruleStateDescriptor).immutableEntries()) {
                            final String ruleName = entry.getKey();
                            final Rule rule = entry.getValue();
                            List<Item> stored = state.get(ruleName);

                            if (stored == null) {
                                stored = new ArrayList<>();
                            }

                            if (shape.getShp().equals(rule.second   .getShp()) && !stored.isEmpty()) {
                                for (Item i : stored) {
                                    out.collect("MATCH: " + i + " - " + value);
                                }
                                stored.clear();
                            }

                            // there is no else{} to cover if rule.first == rule.second
                            if (shape.getShp().equals(rule.first.getShp())) {
                                stored.add(value);
                            }

                            if (stored.isEmpty()) {
                                state.remove(ruleName);
                                System.out.println("hell?");
                            } else {
                                state.put(ruleName, stored);
                            }
                        }
                    }

                    /**
                     * 注意到这个方法就干了一件事，也就是把广播流中的数据全部塞到了broadcast map state状态中去了，而不是将其输出了
                     * 这样做是为了在processElement中获取Rule流中的规则
                     * @param rule
                     * @param context
                     * @param collector
                     * @throws Exception
                     */
                    @Override
                    public void processBroadcastElement(Rule rule, Context context, Collector<String> collector) throws Exception {
                        context.getBroadcastState(ruleStateDescriptor).put(rule.name, rule);
                    }
                });

        resStream.print();
        env.execute();
    }
}

class Item {
    Color color;
    Shape shape;

    public Color getColor() {
        return color;
    }

    public void setColor(Color color) {
        this.color = color;
    }

    public Shape getShape() {
        return shape;
    }

    public void setShape(Shape shape) {
        this.shape = shape;
    }

    @Override
    public String toString() {
        return "Item{" +
                "color=" + color.getCol() +
                ", shape=" + shape.getShp() +
                '}';
    }
}

class Color {
    String col;

    public String getCol() {
        return col;
    }

    public void setCol(String col) {
        this.col = col;
    }

    @Override
    public String toString() {
        return "Color{" +
                "col='" + col + '\'' +
                '}';
    }
}

class Shape {
    String shp;

    public Shape(String shp) {
        this.shp = shp;
    }

    public String getShp() {
        return shp;
    }

    public void setShp(String shp) {
        this.shp = shp;
    }

    @Override
    public String toString() {
        return "Shape{" +
                "shp='" + shp + '\'' +
                '}';
    }
}

class Rule {
    String name;
    Shape first;
    Shape second;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Shape getFirst() {
        return first;
    }

    public void setFirst(Shape first) {
        this.first = first;
    }

    public Shape getSecond() {
        return second;
    }

    public void setSecond(Shape second) {
        this.second = second;
    }
}

class ItemsSource implements SourceFunction<Item> {
    boolean flag = true;

    @Override
    public void run(SourceContext<Item> sourceContext) throws Exception {

        while (flag) {
            String[] colors = new String[]{"blue", "yellow", "gray", "black", "red", "orange", "green", "white", "gold"};
            String[] shapes = new String[]{"triangle", "rectangle", "circle", "unknown"};
            Random random = new Random();
            int colorIndex = random.nextInt(8);
            int shapeIndex = random.nextInt(4);
            Item item = new Item();
            Shape shape = new Shape(shapes[shapeIndex]);
            Color color = new Color();
            color.setCol(colors[colorIndex]);
            item.setColor(color);
            item.setShape(shape);

            sourceContext.collect(item);
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}

class RuleSource implements SourceFunction<Rule> {
    boolean flag = true;

    @Override
    public void run(SourceContext<Rule> sourceContext) throws Exception {
        while (flag) {
            Rule rule = new Rule();
            String[] shapes = new String[]{"unknown", "circle", "rectangle","triangle"};
            Random random = new Random();
            int index1 = random.nextInt(4);
            int index2 = random.nextInt(4);
            rule.setName(UUID.randomUUID().toString());
            rule.setFirst(new Shape(shapes[index1]));
            rule.setSecond(new Shape(shapes[index2]));
            sourceContext.collect(rule);
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}
