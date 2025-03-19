package com.zyu.window04;

import com.zyu.bean.WaterSensor;
import com.zyu.func.WaterSensorMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author zyu
 * date2025/3/18 10:20
 */
public class Flink01_Window_KeyBy {
    public static void main(String[] args) throws Exception {
        // 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从指定的端口获取数据
        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 8888);
        // 将流中数据进行类型专精
        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(new WaterSensorMapFunction());
        // 开窗
        // no-keyBy 针对整条流进行开窗，不使用keyBy，所有数据在同一个窗口处理，相当于并行度为1
/*        wsDS
                // 使用计数窗口, 每5条数据一个窗口
                .countWindowAll(5)
                // 使用滚动窗口, 每5秒触发一次
                // .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum("vc")
                .print("no-keyBy: ");*/
        // keyBy
        wsDS
                .keyBy(WaterSensor::getId)
                // .countWindow(5)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum("vc")
                .print("with-keyBy");

        // 提交作业
        env.execute();
    }
}