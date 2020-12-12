package it.kenn.eventtime;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * 周期性触发，依靠新数据
 */
public class BoundedOutOfOrdernessGenerator implements WatermarkGenerator<JSONObject> {

    private final Long maxOutofOrderness = 3500L;
    private long currMaxTs;

    @Override
    public void onEvent(JSONObject event, long eventTs, WatermarkOutput watermarkOutput) {
        currMaxTs = Math.max(currMaxTs, eventTs);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
        watermarkOutput.emitWatermark(new Watermark(currMaxTs - maxOutofOrderness - 1));
    }
}
