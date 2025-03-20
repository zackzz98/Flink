package com.zyu.sour_tran;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zyu
 * date2025/3/10 17:10
 * 本案例展示了源算子 - 数据生成器
 * 启动这个程序注释掉了pom.xml文件中的两个provided配置，一个是 flink-streaming-java, 一个是 flink-clients
 */
public class Flink07_Source_DataGen {
    public static void main(String[] args) throws Exception {
        // 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 通过数据生成器生成数据
        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, String>() {
                    @Override
                    public String map(Long value) throws Exception {
                        return "数据 -> " + value;
                    }
                },
                100,
                RateLimiterStrategy.perSecond(10),
                TypeInformation.of(String.class)
        );
        // 封装为流
        DataStreamSource<String> ds = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data_Gen_Source");
        // 打印输出
        ds.print();
        // 提交作业
        env.execute();
    }
}