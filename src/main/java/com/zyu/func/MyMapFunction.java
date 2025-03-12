package com.zyu.func;

import com.zyu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author zyu
 * date2025/3/11 14:37
 */
public class MyMapFunction implements MapFunction<WaterSensor,String> {
    @Override
    public String map(WaterSensor ws) throws Exception {
        return ws.getId();
    }
}