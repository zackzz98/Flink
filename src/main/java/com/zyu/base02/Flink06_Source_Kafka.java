package com.zyu.base02;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * @author zyu
 * date2025/3/6 17:26
 * 本案例展示了源算子 - 从Kafka中读取数据
 */
public class Flink06_Source_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从Kafka主题中读取数据
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("bg01:9092,bg02:9092,bg03:9092")
                .setTopics("first")
                .setGroupId("test")
                // 从最早位点开始消费
                // .setStartingOffsets(OffsetsInitializer.earliest())
                // 从最末尾位点开始消费
                // .setStartingOffsets(OffsetsInitializer.latest())
                // 从时间戳大于等于指定时间戳（毫秒）的数据开始消费
                // .setStartingOffsets(OffsetsInitializer.timestamp(1657256176000L))
                // 从消费组提交的位点开始消费，不指定位点重置策略
                // .setStartingOffsets(OffsetsInitializer.committedOffsets())
                // 从消费组提交的位点开始消费，如果提交位点不存在，使用最早位点
                // .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        // 封装为流
        DataStreamSource<String> ds = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");
        // 打印输出
        ds.print();
        // 提交作业
        env.execute();
    }
}