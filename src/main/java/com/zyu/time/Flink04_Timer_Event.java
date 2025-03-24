package com.zyu.time;

import com.zyu.bean.WaterSensor;
import com.zyu.func.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author zyu
 * date2025/3/24 15:18
 * 本案例展示了事件时间定时器
 */
public class Flink04_Timer_Event {
    public static void main(String[] args) throws Exception {
        // 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 从指定的网络端口中获取数据
        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 8888);
        // 对读取的数据进行类型转换
        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(new WaterSensorMapFunction());

        SingleOutputStreamOperator<WaterSensor> watermarkDS = wsDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<WaterSensor>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<WaterSensor>() {
                                    @Override
                                    public long extractTimestamp(WaterSensor ws, long recordTimestamp) {
                                        return ws.getTs();
                                    }
                                }
                        )
        );
        // 按照传感器id进行分组
        KeyedStream<WaterSensor, String> keyedDS = watermarkDS.keyBy(WaterSensor::getId);
        // TODO 注册定时器
        SingleOutputStreamOperator<String> processDS = keyedDS.process(
                new KeyedProcessFunction<String, WaterSensor, String>() {
                    @Override
                    public void processElement(WaterSensor ws, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        // 按照当前分组的key
                        String currentKey = ctx.getCurrentKey();
                        System.out.println("currentKey:" + currentKey);

                        // 获取当前元素的事件时间
                        Long timestamp = ctx.timestamp();
                        System.out.println("timestamp:" + timestamp);

                        // 将数据"放到"侧输出流中
                        // 注意：如果要使用侧输出流, 必须用process算子对流中数据进行处理
                        // ctx.output();

                        // TODO 获取定时服务
                        TimerService timerService = ctx.timerService();

                        long currentProcessingTime = timerService.currentProcessingTime();
                        System.out.println("currentProcessingTime:" + currentProcessingTime);

                        long currentWatermark = timerService.currentWatermark();
                        System.out.println("currentWatermark:" + currentWatermark);

                        // 注册处理时间定时器  定时器的触发是由系统时间触发的
                        // timerService.registerProcessingTimeTimer(currentProcessingTime + 1000);

                        // TODO 注意：同一个时间的定时器只会有一个

                        // 注册事件时间定时器  定时器的触发是由水位线(逻辑时钟)触发的
                        timerService.registerEventTimeTimer(10);

                        // 删除处理时间定时器
                        // timerService.deleteProcessingTimeTimer(10);
                        // 删除事件时间定时器
                        // timerService.deleteEventTimeTimer(10);
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, WaterSensor, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        // 定时器被触发的时候执行的方法
                        out.collect("key是" + ctx.getCurrentKey() + "定时器在" + timestamp + "触发了");
                    }
                }
        );
        // 打印
        processDS.print();
        // 提交作业
        env.execute();
    }
}