package com.zyu.watermark;

import com.zyu.bean.WaterSensor;
import com.zyu.func.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author zyu
 * date2025/3/20 10:31
 * 本案例展示了水位线的生成策略 -- 单调递增
 */
public class Flink01_Monotonous {
    public static void main(String[] args) throws Exception {
        // 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 从指定的网络端口读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 8888);
        // 对流中数据进行类型转换
        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(new WaterSensorMapFunction());
        // 指定Watermark
        SingleOutputStreamOperator<WaterSensor> watermarkDS = wsDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        // 指定Watermark的生成策略
                        .<WaterSensor>forMonotonousTimestamps()
                        // 提取事件时间字段
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                            @Override
                            public long extractTimestamp(WaterSensor ws, long recordTimestamp) {
                                // 返回的单位是毫秒
                                return ws.getTs();
                            }
                        })
        );
        // 按照传感器id进行分组
        KeyedStream<WaterSensor, String> keyedDS = watermarkDS.keyBy(WaterSensor::getId);
        // 开窗
        WindowedStream<WaterSensor, String, TimeWindow> windowDS = keyedDS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        // 对窗口数据进行处理
        SingleOutputStreamOperator<String> processDS = windowDS.process(
                new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        String windowStart = DateFormatUtils.format(context.window().getStart(), "yyyy-MM-dd HH:mm:ss.SSS");
                        String windowEnd = DateFormatUtils.format(context.window().getEnd(), "yyyy-MM-dd HH:mm:ss.SSS");
                        long count = elements.spliterator().estimateSize();
                        out.collect("key=" + s + "的窗口[" + windowStart + "," + windowEnd + ")包含" + count + "条数据===>" + elements.toString());
                    }
                }
        );
        // 打印
        processDS.print();
        // 提交作业
        env.execute();
    }
}