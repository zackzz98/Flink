package com.zyu.window04;

import com.zyu.bean.WaterSensor;
import com.zyu.func.WaterSensorMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author zyu
 * date2025/3/18 13:40
 * 该案例演示了窗口分配器
 */
public class Flink02_Window_Assign {
    public static void main(String[] args) throws Exception {
        // 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从指定的网络端口获取数据
        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 8888);
        // 将流中环境进行类型转换   String -> WaterSensor
        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(new WaterSensorMapFunction());
        // 按照传感器id进行分组
        KeyedStream<WaterSensor, String> keyedStream = wsDS.keyBy(WaterSensor::getId);
        // 开窗 (针对KeyBy后的每一组进行独立开窗)  --- 时间窗口
        // 滚动处理时间窗口
        WindowedStream<WaterSensor, String, TimeWindow> windowDS = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
    }
}