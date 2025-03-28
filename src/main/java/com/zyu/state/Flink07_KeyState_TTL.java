package com.zyu.state;

import com.zyu.bean.WaterSensor;
import com.zyu.func.WaterSensorMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author zyu
 * date2025/3/28 14:37
 * 本案例展示了状态的保留时间的设置
 * 需求：检测每种传感器的水位值，如果连续的两个水位差值超过10，就输出报警
 */
public class Flink07_KeyState_TTL {
    public static void main(String[] args) throws Exception {
        // 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 从指定的网络端口中读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 8888);
        // 对流中的数据进行类型转换
        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(new WaterSensorMapFunction());
        // 按照传感器id进行分组
        KeyedStream<WaterSensor, String> keyedDS = wsDS.keyBy(WaterSensor::getId);
        // 对分组后的数据进行处理
        SingleOutputStreamOperator<String> processDS = keyedDS.process(
                new KeyedProcessFunction<String, WaterSensor, String>() {
                    // 声明值状态
                    // 注意：虽然键控状态声明在成员变量的位置, 但是作用范围不是每一个子任务, 而是keyBy之后的每一个组
                    // 不能在声明的时候直接对状态进行初始化, 因为这个时候算子的生命周期还没有开始, 获取不到运行时上下文对象的
                    ValueState<Integer> lastVcState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 对于键控状态, 是在open方法中进行初始化
                        ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<Integer>("lastVcState", Integer.class);
                        // 注意：这里的时间指的就是系统时间
                        valueStateDescriptor.enableTimeToLive(
                                StateTtlConfig
                                        .newBuilder(Time.seconds(10))
                                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                        .build());
                        lastVcState = getRuntimeContext().getState(valueStateDescriptor);
                        super.open(parameters);
                    }

                    @Override
                    public void processElement(WaterSensor ws, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        // 获取当前水位值
                        Integer curVc = ws.getVc();
                        String id = ctx.getCurrentKey();
                        // 从状态中获取上次水位值
                        Integer lastVc = lastVcState.value() == null ? 0 : lastVcState.value();
                        if (Math.abs(curVc - lastVc) > 10) {
                            out.collect("传感器id" + id + "当前水位值" + curVc + "和上一次水位值" + lastVc + "之差大于10");
                        }
                        // 将当前水位值更新到状态中
                        if (curVc < 30) {
                            lastVcState.update(curVc);
                        }
                    }
                }
        );
        // 打印
        processDS.printToErr();
        // 提交作业
        env.execute();
    }
}