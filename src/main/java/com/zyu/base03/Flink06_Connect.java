package com.zyu.base03;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author zyu
 * date2025/3/14 14:03
 * 本案例展示了合流算子 - connect
 */
public class Flink06_Connect {
    public static void main(String[] args) throws Exception {
        // 环境准备
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        // 从指定的网络端口读取字符串数据
        DataStreamSource<String> ds1 = env.socketTextStream("bg01", 8888);
        // 从指定的网络端口读取字符串数据并转化为Integer
        SingleOutputStreamOperator<Integer> ds2 = env
                .socketTextStream("bg01", 8889)
                .map(Integer::valueOf);

        // 使用connect合流
        ConnectedStreams<String, Integer> connectDS = ds1.connect(ds2);

        // 对连接后的数据进行处理
        SingleOutputStreamOperator<String> processDS = connectDS.process(
                new CoProcessFunction<String, Integer, String>() {
                    @Override
                    public void processElement1(String value, CoProcessFunction<String, Integer, String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect("字符串：" + value);
                    }

                    @Override
                    public void processElement2(Integer value, CoProcessFunction<String, Integer, String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect("数字：" + value);
                    }
                }
        );

        // 打印
        processDS.print();
        // 提交作业
        env.execute();
    }
}