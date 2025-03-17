package com.zyu.sink03;

import com.zyu.bean.WaterSensor;
import com.zyu.func.WaterSensorMapFunction;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * @author zyu
 * date2025/3/17 14:59
 * 本案例展示了将流中数据写到Kafka不同的主题中
 */
public class Flink10_Sink_Kafka_02 {
    public static void main(String[] args) throws Exception {
        // 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从指定的端口读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("bg01", 8888);
        // 对流中数据进行类型转换  String -> WaterSensor
        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(new WaterSensorMapFunction());
        // 将流中数据写到kafka不同主题中  传感器id是ws1 - ws1   ws2 - ws2
        // 创建KafkaSink对象
        KafkaSink<WaterSensor> kafkaSink = KafkaSink.<WaterSensor>builder()
                .setBootstrapServers("bg01:9092")
                .setRecordSerializer(
                        new KafkaRecordSerializationSchema<WaterSensor>() {
                            @Nullable
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(WaterSensor ws, KafkaSinkContext context, Long timestamp) {
                                return new ProducerRecord<>(ws.getId(), ws.toString().getBytes());
                            }
                        }
                )
                .build();
        wsDS.sinkTo(kafkaSink);

        // 提交作业
        env.execute();
    }
}