package it.kenn.eventtime;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * 周期性触发，只要最后生成新的watermark的操作在onPeriodicEmit中的都是周期性触发
 */
public class TimeLagWatermarkGenerator implements WatermarkGenerator<JSONObject> {
    @Override
    public void onEvent(JSONObject event, long l, WatermarkOutput watermarkOutput) {

    }

    @Override
    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
        //认为水位线比当前处理时间晚5s
        watermarkOutput.emitWatermark(new Watermark(System.currentTimeMillis() - 5000));
    }
}
