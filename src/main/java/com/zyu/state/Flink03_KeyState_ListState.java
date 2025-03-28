package com.zyu.state;

import com.zyu.bean.WaterSensor;
import com.zyu.func.MySourceFunction;
import com.zyu.func.WaterSensorMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
*@author zyu
*date2025/3/27 13:26
 * 本案例展示了键控状态-列表状态
 * 需求：针对每种传感器输出最高的3个水位值
*/
public class Flink03_KeyState_ListState {
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
                    // 声明键控状态
                    ListState<Integer> vcListState;

                    // 在open中对键控状态进行初始化
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ListStateDescriptor<Integer> listStateDescriptor = new ListStateDescriptor<Integer>("vcListState", Integer.class);
                        vcListState = getRuntimeContext().getListState(listStateDescriptor);
                    }

                    @Override
                    public void processElement(WaterSensor ws, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        // 获取当前水位值
                        Integer curVc = ws.getVc();
                        // 将当前水位放到状态中
                        vcListState.add(curVc);
                        // 对状态中的数据进行排序
                        List<Integer> vcList = new ArrayList<>();
                        for (Integer vc : vcListState.get()) {
                            vcList.add(vc);
                        }
                        vcList.sort((o1, o2) -> o2 -o1);
                        // 判断结婚中元素的个数  是否大于3
                        if (vcList.size() > 3) {
                            vcList.remove(3);
                        }
                        out.collect("传感器id为" + ws.getId() + ",最大的3个水位值=" + vcList.toString());
                        // 将list更新状态中
                        vcListState.update(vcList);
                    }
                }
        );
        // 打印
        processDS.printToErr();
        // 提交作业
        env.execute();
    }
}