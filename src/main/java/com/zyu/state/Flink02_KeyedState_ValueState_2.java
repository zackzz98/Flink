package com.zyu.state;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zyu
 * date2025/3/25 18:37
 *
 */
public class Flink02_KeyedState_ValueState_2 {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //
    }
}