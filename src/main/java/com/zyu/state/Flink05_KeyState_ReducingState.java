package com.zyu.state;

import com.zyu.bean.WaterSensor;
import com.zyu.func.MySourceFunction;
import com.zyu.func.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author zyu
 * date2025/3/28 10:39
 * 本案例展示了键控状态-规约状态
 * 需求：计算每种传感器的水位和
 */
public class Flink05_KeyState_ReducingState {
    public static void main(String[] args) throws Exception {
        // 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        // 从指定的网络端口读取数据
//        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 8888);
//        // 对流中的数据进行类型转换
//        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(new WaterSensorMapFunction());
        DataStreamSource<WaterSensor> wsDS = env.addSource(new MySourceFunction());
        // 按照传感器id进行分组
        KeyedStream<WaterSensor, String> keyedDS = wsDS.keyBy(WaterSensor::getId);
        // 对分组后的数据进行处理
        SingleOutputStreamOperator<String> processDS = keyedDS.process(
                new KeyedProcessFunction<String, WaterSensor, String>() {
                    ReducingState<Integer> vcSumState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ReducingStateDescriptor<Integer> reducingStateDescriptor = new ReducingStateDescriptor<Integer>(
                                "vcSumState",
                                new ReduceFunction<Integer>() {
                                    @Override
                                    public Integer reduce(Integer value1, Integer value2) throws Exception {
                                        return value1 + value2;
                                    }
                                },
                                Integer.class
                        );
                        vcSumState = getRuntimeContext().getReducingState(reducingStateDescriptor);
                    }

                    @Override
                    public void processElement(WaterSensor ws, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        Integer vc = ws.getVc();
                        // 将当前水位和原来状态中的结果进行累加
                        vcSumState.add(vc);

                        // 从状态中获取累计结果
                        Integer sum = vcSumState.get();
                        out.collect("当前传感器" + ctx.getCurrentKey() + "水位和" + sum);
                    }
                }
        );
        // 打印
        processDS.printToErr();
        // 提交作业
        env.execute();
    }
}