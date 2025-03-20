package com.zyu.window;

import com.zyu.bean.WaterSensor;
import com.zyu.func.MySourceFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author zyu
 * date2025/3/19 10:17
 * 本案例展示了窗口处理函数 apply
 * 需求：每隔10s,统计不同的传感器采集的水位信息
 */
public class Flink05_Window_Apply {
    public static void main(String[] args) throws Exception {
        // 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从指定的网络端口读取数据
//        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 8888);
        // 将流中数据进行类型转换   String -> WaterSensor
//        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(new WaterSensorMapFunction());
        // 这里使用了自定义数据输入,共输入100条数据,用来看效果展示
        DataStreamSource<WaterSensor> socketDS = env.addSource(new MySourceFunction());
        // 根据传感器id进行分组
        KeyedStream<WaterSensor, String> keyedStream = socketDS.keyBy(WaterSensor::getId);
        // 开窗
        WindowedStream<WaterSensor, String, TimeWindow> windowsDS = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        // 对窗口数据进行处理 -- 全量处理数据 -- apply
        SingleOutputStreamOperator<String> applyDS = windowsDS.apply(
                new WindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<WaterSensor> input, Collector<String> out) throws Exception {
                        String windowStart = DateFormatUtils.format(window.getStart(), "yyyy-MM-dd HH:mm:ss.SSS");
                        String windowEnd = DateFormatUtils.format(window.getEnd(), "yyyy-MM-dd HH:mm:ss.SSS");
                        // Spliterator 是 Java 8 引入的一个接口，提供了一种遍历和分割元素的方式，类似于 Iterator，但支持并行处理
                        // estimateSize() 用于获取当前窗口中元素的数量
                        long count = input.spliterator().estimateSize();
                        out.collect("key=" + s + "的窗口[" + windowStart + "," + windowEnd + ")包含" + count + "条数据 ===>" + input.toString());
                    }
                }
        );
        // 打印
        applyDS.print();
        // 提交作业
        env.execute();
    }
}