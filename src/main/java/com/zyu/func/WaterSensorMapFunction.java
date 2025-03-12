package com.zyu.func;

import com.zyu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author zyu
 * date2025/3/12 15:13
 * 将字符串封装为WaterSensor对象
 */
public class WaterSensorMapFunction implements MapFunction<String, WaterSensor> {
    @Override
    public WaterSensor map(String lineStr) throws Exception {
        String[] fieldArr = lineStr.split(",");
        return new WaterSensor(fieldArr[0], Long.valueOf(fieldArr[1]), Integer.valueOf(fieldArr[2]));
    }
}