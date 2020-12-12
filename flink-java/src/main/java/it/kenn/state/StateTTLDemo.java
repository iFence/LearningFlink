package it.kenn.state;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;

/**
 * 状态过期时间配置，ttl即time-to-live嘛
 */
public class StateTTLDemo {
    public static void main(String[] args) {
        StateTtlConfig ttlConfig = StateTtlConfig
                //过期时间，必须的值
                .newBuilder(Time.seconds(10))
                //默认值
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                //配置如果状态已经过期了是否返回，下面的配置是说如果状态过期了就不返回了
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        ValueStateDescriptor<String> text_state = new ValueStateDescriptor<>("text state", String.class);
        text_state.enableTimeToLive(ttlConfig);
    }
}
