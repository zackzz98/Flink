package com.zyu.sour_tran;

import com.zyu.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author zyu
 * date2025/3/12 14:19
 * 本案例展示了转换算子 - flatmap
 * 需求：如果输入的数据是sensor_1，只打印vc；如果输入的数据是sensor_2，既打印ts又打印vc。
 */
public class Flink11_Trans_FlatMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 准备数据
        DataStreamSource<WaterSensor> ds = env.fromElements(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_1", 2L, 2),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_3", 3L, 3)
        );

        // 处理数据
        // 如果使用map对流中数据进行处理，通过map方法的返回值向下游传递数据，返回值只有一个，所以只能向下游传递一次数据
/*        SingleOutputStreamOperator<String> rs = ds.map(
                new MapFunction<WaterSensor, String>() {
                    @Override
                    public String map(WaterSensor value) throws Exception {
                        return null;
                    }
                }
        );*/

        // 如果使用flatmap对流中数据进行处理，不是通过返回值传递数据，是通过Collector向下游传递数据，可以传递多次
        SingleOutputStreamOperator<String> rs = ds.flatMap(
                new FlatMapFunction<WaterSensor, String>() {
                    @Override
                    public void flatMap(WaterSensor ws, Collector<String> out) throws Exception {
                        String id = ws.getId();
                        if ("sensor_1".equals(id)) {
                            out.collect("传感器" + id + "的vc值为: " + ws.getVc());
                        } else if ("sensor_2".equals(id)) {
                            out.collect("传感器" + id + "的ts值为: " + ws.getTs());
                            out.collect("传感器" + id + "的vc值为: " + ws.getVc());
                        }
                    }
                }
        );

        // 打印
        rs.print();
        // 提交作业
        env.execute();
    }
}