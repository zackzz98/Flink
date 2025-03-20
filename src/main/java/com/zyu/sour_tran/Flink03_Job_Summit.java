package com.zyu.sour_tran;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zyu
 * date2025/3/6 16:27
 * 本案例展示了作业提交
 */
public class Flink03_Job_Summit {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从指定的网络端口读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("bg01", 8888);
        // 打印输出
        socketDS.print();
        // 同步提交作业
        // env.execute();
        // 异步提交作业
        env.executeAsync();
        // -------------------------------
        StreamExecutionEnvironment env1 = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从指定的端口读取数据
        DataStreamSource<String> socketDS1 = env1.socketTextStream("bg01", 8889);
        // 打印输出
        socketDS1.print();
        // 同步提交作业
        // env1.execute();
        // 异步提交作业
        env1.executeAsync();
    }
}