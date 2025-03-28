package com.zyu.state;

import com.zyu.bean.WaterSensor;
import com.zyu.func.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zyu
 * date2025/3/28 15:56
 * 本案例展示了算子状态-ListState
 * 需求：在map算子中的每个并行度上计算数据的个数
 */
public class Flink09_OperState_ListState {
    public static void main(String[] args) throws Exception {
        // 准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(10000);
        // 从指定的网络端口读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 8888);
        // 对流中数据进行类型转换  String -> WaterSensor
        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(new WaterSensorMapFunction());
        // 对转换后的数据进行处理
        SingleOutputStreamOperator<String> mapDS = wsDS.map(new MyMap());
        // 打印
        mapDS.printToErr();
        // 提交作业
        env.execute();

    }
}


class MyMap extends RichMapFunction<WaterSensor, String> implements CheckpointedFunction {
    Integer count = 0;
    ListState<Integer> countState;

    @Override
    public String map(WaterSensor ws) throws Exception {
        return "并行子任务：" + getRuntimeContext().getIndexOfThisSubtask() + "处理了" + ++count + "条数据";
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        System.out.println("======snapshotState======");
        countState.clear();
        countState.add(count);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        System.out.println("======initializeState======");
        ListStateDescriptor<Integer> listStateDescriptor = new ListStateDescriptor<Integer>("countState", Integer.class);
        countState = context.getOperatorStateStore().getListState(listStateDescriptor);

        if (context.isRestored()) {
            Integer restorCount = countState.get().iterator().next();
            count = restorCount;
        }
    }
}