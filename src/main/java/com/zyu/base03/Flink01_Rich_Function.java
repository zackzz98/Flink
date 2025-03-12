package com.zyu.base03;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

/**
 * @author zyu
 * date2025/3/12 16:23
 * 本案例展示了富函数类
 * 需求：从指定的网络端口读取数据，给读取的每条数据加"zyu-"前缀再向下游传递
 *
 *
 */
public class Flink01_Rich_Function {
    public static void main(String[] args) throws Exception {
        // 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        // 从指定的网络端口读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("bg01", 8888);

/*        SingleOutputStreamOperator<String> mapDS = socketDS.map(
                new RichMapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        return "zyu-" + value;
                    }
                }
        );*/

        SingleOutputStreamOperator<String> mapDS = socketDS.map(
                new RichMapFunction<String, String>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        System.out.println("== open ==");
                        int index = getRuntimeContext().getIndexOfThisSubtask();
                        String name = getRuntimeContext().getTaskNameWithSubtasks();
                        System.out.println("索引号是"+index+"的子任务["+name+"]正在初始化");
                    }
                    @Override
                    public void close() throws Exception {
                        System.out.println("== close ==");
                    }
                    @Override
                    public String map(String value) throws Exception {
                        return "zyu-" + value;
                    }
                }
        );

        // 打印
        mapDS.print();
        // 提交作业
        env.execute();
    }
}