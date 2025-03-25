package com.zyu.state;

import com.zyu.bean.WaterSensor;
import com.zyu.func.WaterSensorMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * @author zyu
 * date2025/3/25 17:44
 * 本案例展示了键控状态-值状态
 * 需求：检测每种传感器的水位值,如果连续的两个水位差值超过10,就输出报警
 */
public class Flink01_KeyedState_ValueState_1 {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 从指定的网络端口中获取数据
        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 8888);
        // 对流中的数据进行类型转换
        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(new WaterSensorMapFunction());
        // 按照传感器ID进行分组
        KeyedStream<WaterSensor, String> keyedDS = wsDS.keyBy(WaterSensor::getId);
        // 对分组后的数据进行处理
        SingleOutputStreamOperator<String> processDS = keyedDS.process(
                new KeyedProcessFunction<String, WaterSensor, String>() {
                    // 如果用普通的变量记录上次水位值，其作用访问值算子子任务，在一个子任务中的多个组共享当前变量
                    // Integer lastVc = 0
                    Map<String, Integer> lastVcMap = new HashMap<>();

                    @Override
                    public void processElement(WaterSensor ws, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        Integer curVc = ws.getVc();
                        String id = ctx.getCurrentKey();
                        Integer lastVc = lastVcMap.get(id);
                        lastVc = lastVc == null ? 0 : lastVc;
                        if (Math.abs(curVc - lastVc) > 10) {
                            out.collect("传感器id:" + id + "当前水位值" + curVc + "和上一次水位值" + lastVc + "之差大于10");
                        }
                        lastVcMap.put(id, curVc);
                    }
                }
        );
        // 打印
        processDS.printToErr();
        // 提交作业
        env.execute();
    }
}