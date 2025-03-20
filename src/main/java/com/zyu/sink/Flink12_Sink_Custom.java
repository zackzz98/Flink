package com.zyu.sink;

import com.zyu.bean.WaterSensor;
import com.zyu.func.WaterSensorMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author zyu
 * date2025/3/17 17:46
 * 本案例展示了自定义Sink - 将流中数据写到MySQL数据库
 */
public class Flink12_Sink_Custom {
    public static void main(String[] args) throws Exception {
        // 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从指定的网络端口读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("bg01", 8888);
        // 对流中数据进行类型转换
        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(new WaterSensorMapFunction());
        // 将数据写到MySQL中
        wsDS.addSink(new MySinkFunction());
        // 提交作业
        env.execute();
    }
}

class MySinkFunction extends RichSinkFunction<WaterSensor> {
    Connection conn;
    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("===open===");
        // 注册驱动
        Class.forName("com.mysql.cj.jdbc.Driver");
        // 建立连接
        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/zyu?characterEncoding=utf8&autoReconnect=true&serverTimezone=Asia/Shanghai","root", "123456");
    }

    @Override
    public void close() throws Exception {
        System.out.println("===close===");
        conn.close();
    }

    @Override
    public void invoke(WaterSensor ws, Context context) throws Exception {
        // 获取数据库操作对象
        String sql = "insert into ws values(?,?,?)";
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setString(1,ws.getId());
        ps.setLong(2,ws.getTs());
        ps.setString(3,ws.getVc() + "");
        // 执行sql语句
        ps.execute();
        // 释放资源
        ps.close();
    }
}