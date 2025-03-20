package com.zyu.sour_tran;

import com.zyu.bean.WaterSensor;
import com.zyu.func.WaterSensorMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zyu
 * date2025/3/12 15:34
 * 本案例展示了聚合算子 - min 和 minBy
 * 需求：统计不同传感器采集的水位最小值
 * min minBy max maxBy
 * 区别：min返回最小值，minBy返回该字段中具有最小值的元素 (max和maxBy也是如此)
 */
public class Flink13_Agg_Min_MinBy {
    public static void main(String[] args) throws Exception {
        // 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从指定的网络端口读取数据   ws1,10,10
        DataStreamSource<String> socketDS = env.socketTextStream("bg01", 8888);
        // 对流中数据进行转换   String -> WaterSensor
        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(
                new WaterSensorMapFunction()
        );
        //按照传感器id进行分组
        KeyedStream<WaterSensor, String> keyedDS = wsDS.keyBy(
                new KeySelector<WaterSensor, String>() {
                    @Override
                    public String getKey(WaterSensor ws) throws Exception {
                        return ws.getId();
                    }
                }
        );
        // 取最小值
//        SingleOutputStreamOperator<WaterSensor> minDS = keyedDS.min("vc");
        SingleOutputStreamOperator<WaterSensor> minDS = keyedDS.minBy("vc");
        // 打印
        minDS.print();
        // 提交作业
        env.execute();
    }
}