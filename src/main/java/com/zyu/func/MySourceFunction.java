package com.zyu.func;

import com.zyu.bean.WaterSensor;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @author zyu
 * date2025/3/19 10:39
 */
public class MySourceFunction implements SourceFunction<WaterSensor> {
    private volatile boolean isRunning = true;
    private static final String[] SENSOR_IDS = {"ws1", "ws2", "ws3"};
    private static final Random RANDOM = new Random();

    @Override
    public void run(SourceContext<WaterSensor> ctx) throws Exception {
        int count = 0;
        while (isRunning && count < 100) {
            // 随机选择一个传感器id
            String id = SENSOR_IDS[RANDOM.nextInt(SENSOR_IDS.length)];
            // 生成随机的水位值
            int vc = RANDOM.nextInt(100);
            // 生成当前时间戳
            long timestamp = System.currentTimeMillis();
            // 收集数据
            ctx.collect(new WaterSensor(id, timestamp, vc));
            count++;
            // 控制生成速度
            Thread.sleep(500);
        }
    }

//    @Override
//    public void run(SourceContext<WaterSensor> ctx) throws Exception {
//        while (isRunning) {
//            ctx.collect(new WaterSensor("ws1", System.currentTimeMillis(), (int)(Math.random() *100)));
//        }
//    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}