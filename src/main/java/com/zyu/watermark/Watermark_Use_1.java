package com.zyu.watermark;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.text.SimpleDateFormat;
import java.time.Duration;

/**
 * @author zyu
 * date2025/3/21 10:43
 */
public class Watermark_Use_1 {
    public static void main(String[] args) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 定义WatermarkStrategy  允许5秒乱序
        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(3)) // 允许3秒乱序
                .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        String[] arr = element.split(",");
                        long timestamp = Long.parseLong(arr[1]);
                        System.out.println("Key:" + arr[0] + ", EventTime: " + timestamp + "(" + sdf.format(timestamp) + ")");
                        return timestamp; // 提取事件时间
                    }
                })
                .withIdleness(Duration.ofSeconds(20)); // 设置空闲source为20秒

        // 应用WatermarkStrategy
        env.socketTextStream("localhost", 9999)
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .map(new MapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(String s) throws Exception {
                        return new Tuple2<>(s.split(",")[0], s.split(",")[1]);
                    }
                })
                .keyBy(value -> value.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(5)) // 允许5秒的延迟数据
                .reduce(new ReduceFunction<Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> reduce(Tuple2<String, String> value1, Tuple2<String, String> value2) throws Exception {
                        return new Tuple2<>(value1.f0, value1.f1 + " - " + value2.f1);
                    }
                })
                .print("reduce结果");

        env.execute("WaterMark Test Demo");
    }
}