package com.zyu.state;

import com.zyu.bean.WaterSensor;
import com.zyu.func.MySourceFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * @author zyu
 * date2025/3/27 14:43
 * 本案例展示了键控状态-Map状态
 * 需求：统计每种传感器每种水位值出现的次数
 */
public class Flink04_KeyState_MapState {
    public static void main(String[] args) throws Exception {
        // 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 使用自定义数据源
        DataStreamSource<WaterSensor> wsDS = env.addSource(new MySourceFunction());
        // 按照传感器id进行分组
        KeyedStream<WaterSensor, String> keyedDS = wsDS.keyBy(WaterSensor::getId);
        // 对分组后的数据进行处理
        SingleOutputStreamOperator<String> processDS = keyedDS.process(
                new KeyedProcessFunction<String, WaterSensor, String>() {
                    MapState<Integer, Integer> vcCountMapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor<Integer, Integer> mapStateDescriptor = new MapStateDescriptor<Integer, Integer>("vcCountMapState", Integer.class, Integer.class);
                        vcCountMapState = getRuntimeContext().getMapState(mapStateDescriptor);
                    }

                    @Override
                    public void processElement(WaterSensor ws, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        Integer vc = ws.getVc();
                        // 判断状态中是否存在当前水位
                        if (vcCountMapState.contains(vc)) {
                            vcCountMapState.put(vc, vcCountMapState.get(vc) + 1);
                        } else {
                            vcCountMapState.put(vc, 1);
                        }
                        // 遍历Map状态, 输出每个k-v的值
                        StringBuilder outStr = new StringBuilder();
                        outStr.append("=============================\n");
                        outStr.append("传感器id为" + ws.getId() + "\n");
                        for (Map.Entry<Integer, Integer> vcCount : vcCountMapState.entries()){
                            outStr.append(vcCount.toString() + "\n");
                        }
                        outStr.append("=============================\n");

                        out.collect(outStr.toString());
                    }
                }
        );
        // 打印
        processDS.printToErr();
        // 提交作业
        env.execute();
    }
}