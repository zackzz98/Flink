package com.zyu.state;

import com.zyu.bean.WaterSensor;
import com.zyu.func.MySourceFunction;
import com.zyu.func.WaterSensorMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author zyu
 * date2025/4/1 15:01
 * 本案例展示了算子状态-广播状态
 * 需求：水位超过指定的阈值发送告警,阈值可以动态修改
 */
public class Flink10_OperState_BroadcastState {
    public static void main(String[] args) throws Exception {
        // 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // 从指定的网络端口读取数据
        SingleOutputStreamOperator<WaterSensor> wsDS = env.socketTextStream("localhost", 8888).map(new WaterSensorMapFunction());
        // 从指定的网络端口读取阈值信息并进行类型转换  String -> Integer
        SingleOutputStreamOperator<Integer> thresholdDS = env.socketTextStream("localhost", 8887).map(Integer::valueOf);
        // 对阈值流数据进行广播 并声明广播状态描述器
        MapStateDescriptor<String, Integer> mapStateDescriptor
                = new MapStateDescriptor<String, Integer>("mapStateDescriptor", String.class, Integer.class);
        BroadcastStream<Integer> broadcastDS = thresholdDS.broadcast(mapStateDescriptor);
        // 关联非广播流(水位信息)以及广播流(阈值) --- connect
        BroadcastConnectedStream<WaterSensor, Integer> connectDS = wsDS.connect(broadcastDS);
        // 对关联后的数据进行处理 -- process
        SingleOutputStreamOperator<String> processDS = connectDS.process(
                new BroadcastProcessFunction<WaterSensor, Integer, String>() {
                    // processElement:处理广播流数据    从广播状态中获取阈值信息判断是否超过警戒线
                    @Override
                    public void processElement(WaterSensor ws, BroadcastProcessFunction<WaterSensor, Integer, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        // 获取广播状态
                        ReadOnlyBroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                        // 从广播状态中获取阈值信息
                        Integer threshold = broadcastState.get("threshold") == null ? 0 : broadcastState.get("threshold");
                        // 获取当前采集的水位值
                        Integer vc = ws.getVc();
                        if (vc > threshold) {
                            out.collect("当前水位" + vc + "超过阈值" + threshold);
                        }
                    }

                    // processBroadcastElement:处理广播流状态    将广播流中的阈值信息放到广播状态中
                    @Override
                    public void processBroadcastElement(Integer threshold, BroadcastProcessFunction<WaterSensor, Integer, String>.Context ctx, Collector<String> out) throws Exception {
                        // 获取广播状态
                        BroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                        // 将广播流中的阈值信息放到广播流中
                        broadcastState.put("threshold", threshold);
                    }
                }
        );
        // 打印
        processDS.print();
        // 提交作业
        env.execute();
    }
}