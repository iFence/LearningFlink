package it.kenn.eventtime;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.*;

public class MyWatermarkStrategy implements WatermarkStrategy<JSONObject> {
    @Override
    public WatermarkGenerator<JSONObject> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {

        return new PunctuatedAssigner();
    }

    @Override
    public TimestampAssigner<JSONObject> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return null;
    }
}
