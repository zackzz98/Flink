package com.zyu.watermark;

import com.zyu.bean.WaterSensor;
import com.zyu.func.MySourceFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author zyu
 * date2025/3/21 14:53
 * 本案例展示了水位线的传递--2
 */
public class Flink05_pass_2 {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为2
        env.setParallelism(2);
        // 获取数据
//        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 8888);
        DataStreamSource<WaterSensor> customDS = env.addSource(new MySourceFunction());

//        // 指定Watermark的生成策略&提取事件时间字段
//        SingleOutputStreamOperator<String> watermarkDS = socketDS.assignTimestampsAndWatermarks(
//                WatermarkStrategy
//                        .<String>forMonotonousTimestamps()
//                        .withTimestampAssigner(
//                                new SerializableTimestampAssigner<String>() {
//                                    @Override
//                                    public long extractTimestamp(String lineStr, long recordTimestamp) {
//                                        String[] fieldArr = lineStr.split(",");
//                                        return Long.valueOf(fieldArr[1]);
//                                    }
//                                }
//                        )
//        );

        SingleOutputStreamOperator<WaterSensor> watermarkDS = customDS.assignTimestampsAndWatermarks(
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
//        // 对流中的数据进行类型转换
//        SingleOutputStreamOperator<WaterSensor> wsDS = watermarkDS.map(new WaterSensorMapFunction());
        // 按照传感器id进行分组
        KeyedStream<WaterSensor, String> keyedDS = watermarkDS.keyBy(WaterSensor::getId);
        // 开窗 -- 滚动事件时间窗口
        WindowedStream<WaterSensor, String, TimeWindow> windowDS = keyedDS.window(TumblingEventTimeWindows.of(Time.seconds(10)));
        // 聚合计算
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