package com.zyu.state;

import com.zyu.bean.WaterSensor;
import com.zyu.func.MySourceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zyu
 * date2025/3/28 15:25
 * 本案例展示了算子状态
 * 需求：在map算子中的每个并行度上计算数据的个数
 */
public class Flink08_OperState_Var {
    public static void main(String[] args) throws Exception {
        // 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // 从指定的网络端口读取数据
        DataStreamSource<WaterSensor> wsDS = env.addSource(new MySourceFunction());
//        // 不需要分组？
//        KeyedStream<WaterSensor, String> keyedDS = wsDS.keyBy(WaterSensor::getId);
        // 对数据进行处理
        SingleOutputStreamOperator<String> mapDS = wsDS.map(
                new RichMapFunction<WaterSensor, String>() {
                    Integer count = 0;

                    @Override
                    public String map(WaterSensor ws) throws Exception {
                        return "并行子任务：" + getRuntimeContext().getIndexOfThisSubtask() + "处理了" + ++count + "条数据";
                    }
                }
        );
        // 打印
        mapDS.printToErr();
        // 提交作业
        env.execute();
    }
}