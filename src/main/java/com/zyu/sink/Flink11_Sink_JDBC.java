package com.zyu.sink;

import com.zyu.bean.WaterSensor;
import com.zyu.func.WaterSensorMapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author zyu
 * date2025/3/17 15:41
 * 本案例展示了将流中数据写到MySQL数据库中
 * 只要是遵循JDBC规范的数据库都可以用这种方式写入
 */
public class Flink11_Sink_JDBC {
    public static void main(String[] args) throws Exception {
        // 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从指定的网络端口读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("bg01", 8888);
        // 对流中数据进行类型转换   String -> WaterSensor
        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(new WaterSensorMapFunction());
        // 将流中数据写到MySQL数据库
        SinkFunction<WaterSensor> sinkFunction = JdbcSink.<WaterSensor>sink(
                "insert into ws value(?,?,?)",
                new JdbcStatementBuilder<WaterSensor>() {
                    @Override
                    public void accept(PreparedStatement ps, WaterSensor ws) throws SQLException {
                        // 给问号占位符进行赋值
                        ps.setString(1,ws.getId());
                        ps.setLong(2,ws.getTs());
                        ps.setString(3,ws.getVc() + "");
                    }
                },
                // 攒批设置
                // new JdbcExecutionOptions.Builder()
                JdbcExecutionOptions
                        .builder()
                        .withBatchSize(5)
                        .withBatchIntervalMs(5000)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUrl("jdbc:mysql://localhost:3306/zyu?characterEncoding=utf8&autoReconnect=true&serverTimezone=Asia/Shanghai")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()
        );
        wsDS.addSink(sinkFunction);

        // 提交作业
        env.execute();
    }
}