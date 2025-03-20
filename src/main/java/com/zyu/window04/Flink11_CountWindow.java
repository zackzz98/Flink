package com.zyu.window04;

import com.zyu.bean.WaterSensor;
import com.zyu.func.MySourceFunction;
import com.zyu.func.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * @author zyu
 * date2025/3/19 16:30
 * 本案例展示了计数窗口
 */
public class Flink11_CountWindow {
    public static void main(String[] args) throws Exception {
        // 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        // 从指定的网络端口中获取数据
//        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 8888);
//        // 对流中数据进行类型转换
//        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(new WaterSensorMapFunction());
        DataStreamSource<WaterSensor> customDS = env.addSource(new MySourceFunction());
        // 按照传感器id进行分组
        KeyedStream<WaterSensor, String> keyedDS = customDS.keyBy(WaterSensor::getId);
        // 开窗
//        WindowedStream<WaterSensor, String, GlobalWindow> windowDS = keyedDS.countWindow(5);
        WindowedStream<WaterSensor, String, GlobalWindow> windowDS = keyedDS.countWindow(10,2);
        // 对窗口数据进行处理
        SingleOutputStreamOperator<String> reduceDS = windowDS.reduce(
                new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                        System.out.println("value1:" + value1);
                        System.out.println("value2:" + value2);
                        return value1;
                    }
                },
                new ProcessWindowFunction<WaterSensor, String, String, GlobalWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, GlobalWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        long count = elements.spliterator().estimateSize();
                        out.collect("key=" + s + "的窗口包含" + count + "条数据===>" + elements.toString());
                    }
                }
        );
        // 打印
        reduceDS.print();
        // 提交作业
        env.execute();
    }
}