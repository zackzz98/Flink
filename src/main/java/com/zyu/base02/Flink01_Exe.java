package com.zyu.base02;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zyu
 * date2025/3/6 15:12
 * 本案例展示了执行环境的创建
 */
public class Flink01_Exe {
    public static void main(String[] args) throws Exception{
        // 创建本地执行环境
        // LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        // 创建本地执行环境带WebUI
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        // 根据实际的场景 动态创建环境
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 根据实际的场景 动态创建环境 本地带WebUI  必须显示设置WebUI的端口号
        // Configuration conf = new Configuration();
        // conf.set(RestOptions.PORT, 8081);
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // 创建远程执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("bg01", 8081, "D:\\dev\\workspace\\bigdata-0422\\target\\bigdata-0422-1.0-SNAPSHOT.jar");
        env
                .readTextFile("/opt/module/flink-1.17.0/words.txt")
                .print();
        env.execute();

    }
}