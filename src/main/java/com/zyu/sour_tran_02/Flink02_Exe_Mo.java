package com.zyu.sour_tran_02;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zyu
 * date2025/3/6 16:03
 * 本案例展示了执行模式 两种方式
 *
 */
public class Flink02_Exe_Mo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置执行模式
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        env
                .readTextFile("/opt/module/flink-1.17.0/words.txt")
                .print();
        env.execute();
    }
}