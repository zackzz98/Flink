package com.zyu.sour_tran;

import com.zyu.bean.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zyu
 * date2025/3/12 13:59
 * 本案例展示了转换算子 - filter（过滤）
 * 需求：过滤除传感器id为sensor_1的
 */
public class Flink10_Trans_Filter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 准备数据
        DataStreamSource<WaterSensor> ds = env.fromElements(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_1", 2L, 2),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_3", 3L, 3),
                new WaterSensor("sensor_1", 3L, 3)
        );
        // 过滤数据
        SingleOutputStreamOperator<WaterSensor> filterDS = ds.filter(
                new FilterFunction<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor ws) throws Exception {
                        return "sensor_1".equals(ws.id);
                    }
                }
        );
        // 打印
        filterDS.print();
        // 提交作业
        env.execute();
    }
}