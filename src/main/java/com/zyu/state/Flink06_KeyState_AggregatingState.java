package com.zyu.state;

import com.zyu.bean.WaterSensor;
import com.zyu.func.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author zyu
 * date2025/3/28 10:54
 * 本案例展示了键控状态-聚合状态
 * 需求：计算每种传感器的平均水位
 */
public class Flink06_KeyState_AggregatingState {
    public static void main(String[] args) throws Exception {
        // 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 从指定的端口读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 8888);
        // 对流中的数据进行类型转换
        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(new WaterSensorMapFunction());
        // 按照传感器id进行分组
        KeyedStream<WaterSensor, String> keyedDS = wsDS.keyBy(WaterSensor::getId);
        // 对分组后的数据进行处理
        SingleOutputStreamOperator<String> processDS = keyedDS.process(
                new KeyedProcessFunction<String, WaterSensor, String>() {
                    AggregatingState<Integer, Double> vcAvgState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        AggregatingStateDescriptor<Integer, Tuple2<Integer, Integer>, Double> aggregatingStateDescriptor = new AggregatingStateDescriptor<Integer, Tuple2<Integer, Integer>, Double>(
                                "vcAvgState",
                                new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {
                                    @Override
                                    public Tuple2<Integer, Integer> createAccumulator() {
                                        return Tuple2.of(0, 0);
                                    }

                                    @Override
                                    public Tuple2<Integer, Integer> add(Integer vc, Tuple2<Integer, Integer> accumulator) {
                                        accumulator.f0 += vc;
                                        accumulator.f1 += 1;
                                        return accumulator;
                                    }

                                    @Override
                                    public Double getResult(Tuple2<Integer, Integer> accumulator) {
                                        Integer vcSum = accumulator.f0;
                                        Integer countSum = accumulator.f1;
                                        return vcSum * 1D / countSum;
                                    }

                                    @Override
                                    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                                        return null;
                                    }
                                },
                                Types.TUPLE(Types.INT, Types.INT)
                        );
                        vcAvgState = getRuntimeContext().getAggregatingState(aggregatingStateDescriptor);
                    }

                    @Override
                    public void processElement(WaterSensor ws, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        Integer vc = ws.getVc();
                        // 将当前水位值放到状态中
                        vcAvgState.add(vc);
                        // 从状态中获取平均水位
                        Double vcAvg = vcAvgState.get();
                        out.collect("传感器" + ctx.getCurrentKey() + "平均水位是" + vcAvg);
                    }
                }
        );
        // 打印
        processDS.printToErr();
        // 提交作业
        env.execute();
    }
}