package com.zyu.sink03;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zyu
 * date2025/3/17 14:49
 * 本案例展示了将流中数据写到Kafka中
 */
public class Flink09_Sink_Kafka_01 {
    public static void main(String[] args) throws Exception {
        // 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从指定的网络端口读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("bg01", 8888);

        // 创建KafkaSink对象
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("bg01:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("xxx") // first
                                // 指定如何将数据序列化为字节 SimpleStringSchema 表示将数据作为普通字符串处理
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                // 设置事务ID的前缀 在使用 EXACTLY_ONCE 语义时需要设置
                .setTransactionalIdPrefix("xxx")
                .build();
        socketDS.sinkTo(kafkaSink);
        // 提交作业
        env.execute();
    }
}