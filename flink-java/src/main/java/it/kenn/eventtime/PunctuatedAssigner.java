package it.kenn.eventtime;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class PunctuatedAssigner implements WatermarkGenerator<JSONObject> {
    @Override
    public void onEvent(JSONObject jsonObject, long eventTimestamp, WatermarkOutput watermarkOutput) {
        //解析时间
        String string = jsonObject.getString("@timestamp");
        DateTimeFormatter pattern = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'hh:mm:ss.SSS'Z'");
        LocalDateTime date = LocalDateTime.parse(string, pattern);
        long currTs = date.toInstant(ZoneOffset.of("+8")).toEpochMilli();
        //将比较当前时间和当前水位线大大小，将大的作为新的watermark
        watermarkOutput.emitWatermark(new Watermark(Math.max(currTs, eventTimestamp)));
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {

    }
}
