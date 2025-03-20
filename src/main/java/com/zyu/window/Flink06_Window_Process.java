package com.zyu.window;

import com.zyu.bean.WaterSensor;
import com.zyu.func.MySourceFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
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
 * date2025/3/19 11:05
 * 本案例展示了窗口处理函数 process
 * 需求：每隔10s,统计不同的传感器采集的水位信息
 */
public class Flink06_Window_Process {
    public static void main(String[] args) throws Exception {
        // 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 使用自定义MySourceFunction
        DataStreamSource<WaterSensor> customDS = env.addSource(new MySourceFunction());
        // 根据传感器id进行分组
        KeyedStream<WaterSensor, String> wsDS = customDS.keyBy(WaterSensor::getId);
        // 开窗
        WindowedStream<WaterSensor, String, TimeWindow> windowDS = wsDS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        // 对数据进行处理 -- 全量处理 -- process
        SingleOutputStreamOperator<String> processDS = windowDS.process(
                new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        String windowStart = DateFormatUtils.format(context.window().getStart(), "yyyy-MM-dd HH:mm:ss.SSS");
                        String windowEnd = DateFormatUtils.format(context.window().getEnd(), "yyyy-MM-dd HH:mm:ss.SSS");
                        long count = elements.spliterator().estimateSize();
                        out.collect("key=" + s + "的窗口[" + windowStart + "," + windowEnd + ")包含" + count + "条数据 ===>" + elements.toString());
                    }
                }
        );
        // 打印
        processDS.print();
        // 提交作业
        env.execute();
    }
}