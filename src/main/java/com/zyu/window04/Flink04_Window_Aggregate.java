package com.zyu.window04;

import com.zyu.bean.WaterSensor;
import com.zyu.func.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
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
 * date2025/3/18 16:29
 * 本案例展示了窗口处理函数 Aggregate
 * 需求：每隔10s,统计不同的传感器采集的水位和
 */
public class Flink04_Window_Aggregate {
    public static void main(String[] args) throws Exception {
        // 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从指定的网络端口中读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 8888);
        // 对流中的数据进行类型转换  String -> WaterSensor
        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(new WaterSensorMapFunction());
        // 根据传感器id进行分组
        KeyedStream<WaterSensor, String> keyedStream = wsDS.keyBy(WaterSensor::getId);
        // 开窗 -- 滚动处理时间窗口
        WindowedStream<WaterSensor, String, TimeWindow> windowDS = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        // 对窗口数据进行处理 -- Aggregate
        SingleOutputStreamOperator<String> aggregateDS = windowDS.aggregate(
                // 泛型接口，定义了三个类型参数
                // WaterSensor：输入数据的类型   Tuple2<Integer, Integer>:累加器的类型,用于在窗口中累积计算结果  String:最终输出结果的类型
                new AggregateFunction<WaterSensor, Tuple2<Integer, Integer>, String>() {
                    @Override
                    // 创建一个新的累加器,在窗口开始时调用,返回一个Tuple2,初始值为(0,0),分别标识累加的水位总和和计数
                    public Tuple2<Integer, Integer> createAccumulator() {
                        System.out.println("==createAccumulator==");
                        return Tuple2.of(0,0);
                    }

                    @Override
                    public Tuple2<Integer, Integer> add(WaterSensor ws, Tuple2<Integer, Integer> accumulator) {
                        // 每当有新的WaterSensor数据到达窗口时调用,累加水位值vc和计数
                        System.out.println("==add==");
                        accumulator.f0 = accumulator.f0 + ws.getVc();
                        accumulator.f1 = accumulator.f1 + 1;
                        return accumulator;
                    }

                    @Override
                    public String getResult(Tuple2<Integer, Integer> accumulator) {
                        // 当窗口触发计算时调用
                        System.out.println("==getResult==");
                        Integer vc = accumulator.f0;
                        Integer count = accumulator.f1;
                        // vc：累加器中存储的水位总和 count：累加器中存储的计数器  1d:是一个double类型的常量,可以确保计算结果是double类型
                        return "平均水位：" + vc * 1d / count;
                    }

                    @Override
                    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                        // 注：只有会话窗口才需要重写merge方法
                        System.out.println("==merge==");
                        return null;
                    }
                }
        );
        // 打印
        aggregateDS.print();
        // 提交作业
        env.execute();
    }
}