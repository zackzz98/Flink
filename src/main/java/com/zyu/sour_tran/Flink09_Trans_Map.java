package com.zyu.sour_tran;

import com.zyu.bean.WaterSensor;
import com.zyu.func.MyMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zyu
 * date2025/3/11 14:42
 * 本案例展示了转换算子 - Map
 * 需求：提取WaterSensor中的id字段
 */
public class Flink09_Trans_Map {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 准备数据
        DataStreamSource<WaterSensor> ds = env.fromElements(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_2", 2L, 2)
        );

        // 好几种方法  提取ws的id
        // 匿名内部类
/*        SingleOutputStreamOperator<String> mapDS = ds.map(
                new MapFunction<WaterSensor, String>() {
                    @Override
                    public String map(WaterSensor ws) throws Exception {
                        return ws.getId();
                    }
                }
        );*/
        // lambda表达式
//        SingleOutputStreamOperator<String> mapDS = ds.map(ws -> ws.id);

        // 方法的引用
//        SingleOutputStreamOperator<String> mapDS = ds.map(WaterSensor::getId);

        // 抽取单独的处理函数类
        SingleOutputStreamOperator<String> mapDS = ds.map(new MyMapFunction());
        // 打印
        mapDS.print();
        // 提交作业
        env.execute();
    }
}