package com.zyu.window;


import com.zyu.bean.WaterSensor;
import com.zyu.func.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author zyu
 * date2025/3/18 14:27
 * 本案例展示了窗口处理函数reduce
 * 需求：每隔10s,统计不同的传感器采集的水位和
 */
public class Flink03_Window_Reduce {
    public static void main(String[] args) throws Exception {
        // 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从指定的网络端口中获取数据
        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 8888);
        // 将流中的数据进行类型转换  String -> WaterSensor
        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(new WaterSensorMapFunction());
        // 根据传感器id进行分组
        KeyedStream<WaterSensor, String> keyedStream = wsDS.keyBy(WaterSensor::getId);
        // 开窗 -- 滚动处理时间窗口
        WindowedStream<WaterSensor, String, TimeWindow> windowsDS = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        // 对窗口数据进行处理 -- 增量处理函数 -- reduce
        SingleOutputStreamOperator<WaterSensor> reduceDS = windowsDS.reduce(
                new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                        System.out.println("中间累加的结果：" + value1);
                        System.out.println("新来的数据：" + value2);
                        value1.setVc(value2.getVc() + value1.getVc());
                        return value1;
                    }
                }
        );
        // 打印
        reduceDS.print();
        // 提交作业
        env.execute();
    }
}