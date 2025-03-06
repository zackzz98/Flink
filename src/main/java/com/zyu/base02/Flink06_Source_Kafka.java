package com.zyu.base02;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
                .setStartingOffsets(OffsetsInitializer.latest())

                .build();
    }
}