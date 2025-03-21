package com.zyu.watermark;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.time.Duration;

/**
 * @author zyu
 * date2025/3/21 13:36
 * 根据Watermark_Use_1 来收集迟到的数据
 */
public class Watermark_Use_2 {
    public static void main(String[] args) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 定义WatermarkStrategy 允许3秒的乱序
        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))  // 允许3秒的乱序
                .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        String[] arr = element.split(",");
                        long timestamp = Long.parseLong(arr[1]);
                        System.out.println("Key: " + arr[0] + ", EventTime: " + timestamp + "(" + sdf.format(timestamp) + ")");
                        return timestamp; // 提取事件时间
                    }
                })
                .withIdleness(Duration.ofSeconds(20));  // 设置空闲source为20秒

        // TODO 保存被丢弃的数据
        OutputTag<Tuple2<String, String>> outputTag = new OutputTag<Tuple2<String, String>>("late-data") {};

        // 应用WatermarkStrategy
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Tuple2<String, String>> resStream = inputStream
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .map(new MapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(String s) throws Exception {
                        return new Tuple2<>(s.split(",")[0], s.split(",")[1]);
                    }
                })
                .keyBy(tuple -> tuple.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sideOutputLateData(outputTag)  // TODO 保存被丢弃的数据
                .reduce(new ReduceFunction<Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> reduce(Tuple2<String, String> t1, Tuple2<String, String> t2) throws Exception {
                        return new Tuple2<>(t1.f0, t1.f1 + " - " + t2.f1);
                    }
                });

        //TODO 把丢弃的数据取出来, 暂时打印到控制台, 开发场景下可以存储到其他存储介质中, 如：redis、kafka
        DataStream<Tuple2<String, String>> sideOutput = resStream.getSideOutput(outputTag);
        sideOutput.print("丢弃的数据");

        // 将流中的数据结果也打印到控制台
        resStream.print("reduce结果");

        env.execute("Watermark Test Demo");
    }
}