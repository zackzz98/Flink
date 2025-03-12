package com.zyu.base02;

import com.zyu.bean.WaterSensor;
import com.zyu.func.WaterSensorMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zyu
 * date2025/3/12 14:48
 * 本案例展示了聚合算子 - sum
 * 需求：统计不同传感器采集的水位和
 */
public class Flink12_Agg_Sum {
    public static void main(String[] args) throws Exception {
        // 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从指定的网络端口读取数据  ws1,10,10
        DataStreamSource<String> socketDS = env.socketTextStream("bg01", 8888);
        // 对流中数据进行转换   String -> WaterSensor
        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(new WaterSensorMapFunction());

        // 按照传感器id进行分组
        KeyedStream<WaterSensor, String> keyedDS = wsDS.keyBy(
                new KeySelector<WaterSensor, String>() {
                    @Override
                    public String getKey(WaterSensor ws) throws Exception {
                        return ws.getId();
                    }
                }
        );

        // 求和
        SingleOutputStreamOperator<WaterSensor> sumDS = keyedDS.sum("vc");
        // 打印
        sumDS.print();
        // 提交作业
        env.execute();
    }
}