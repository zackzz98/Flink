package com.zyu.window04;

import com.zyu.bean.WaterSensor;
import com.zyu.func.MySourceFunction;
import com.zyu.func.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
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
 * date2025/3/19 13:42
 * 本案例展示了窗口处理函数 Aggregate + Process
 * 需求：每隔10s，统计不同的传感器采集的水位信息
 */
public class Flink07_Window_Aggregate_Process {
    public static void main(String[] args) throws Exception {
        // 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        // 从指定的网络端口获取数据
//        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 8888);
//        // 对流中的数据进行类型转换
//        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(new WaterSensorMapFunction());
        DataStreamSource<WaterSensor> customDS = env.addSource(new MySourceFunction());
        // 根据传感器id进行分组
        KeyedStream<WaterSensor, String> keyedDS = customDS.keyBy(WaterSensor::getId);
        // 开窗 -- 滚动处理时间窗口
        WindowedStream<WaterSensor, String, TimeWindow> windowDS = keyedDS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        // 增量 + 全量 对窗口数据进行处理
        SingleOutputStreamOperator<String> aggregateDS = windowDS.aggregate(
                new AggregateFunction<WaterSensor, Tuple2<Integer, Integer>, Double>() {
                    @Override
                    public Tuple2<Integer, Integer> createAccumulator() {
                        System.out.println("==createAccumulator==");
                        return Tuple2.of(0, 0);
                    }

                    @Override
                    public Tuple2<Integer, Integer> add(WaterSensor ws, Tuple2<Integer, Integer> accumulator) {
                        System.out.println("==add==");
                        accumulator.f0 += ws.getVc();
                        accumulator.f1 += 1;
                        return accumulator;
                    }

                    @Override
                    public Double getResult(Tuple2<Integer, Integer> accumulator) {
                        System.out.println("==getResult==");
                        return accumulator.f0 * 1d / accumulator.f1;
                    }

                    @Override
                    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> integerIntegerTuple2, Tuple2<Integer, Integer> acc1) {
                        return null;
                    }
                },
                new ProcessWindowFunction<Double, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<Double, String, String, TimeWindow>.Context context, Iterable<Double> elements, Collector<String> out) throws Exception {
                        System.out.println("==process==");
                        String windowStart = DateFormatUtils.format(context.window().getStart(), "yyyy-MM-dd HH:mm:ss.SSS");
                        String windowEnd = DateFormatUtils.format(context.window().getEnd(), "yyyy-MM-dd HH:mm:ss.SSS");
                        out.collect("key是" + s + "窗口[" + windowStart + "~" + windowEnd + "]平均水位是" + elements.iterator().next());
                    }
                }
        );
        // 打印
        aggregateDS.print();
        // 提交作业
        env.execute();
    }
}