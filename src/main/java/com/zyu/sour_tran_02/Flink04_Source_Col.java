package com.zyu.sour_tran_02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zyu
 * date2025/3/6 16:45
 * 本案例展示了源算子 - 从集合中读取数据
 */
public class Flink04_Source_Col {
    public static void main(String[] args) throws Exception {
        // 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(2);
        // 从集合中读取数据
        // DataStreamSource<Integer> ds = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5)); 这种方式适合已经有一个集合对象
        DataStreamSource<Integer> ds = env.fromElements(1, 2, 3, 4, 5); // 这种适合传入可变参数，比如快速传入几个参数
        // 打印输出
        ds.print();
        // 提交作业
        env.execute();
    }
}