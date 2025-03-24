package com.zyu.time;

import com.zyu.bean.WaterSensor;
import com.zyu.func.WaterSensorMapFunction;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author zyu
 * date2025/3/24 16:00
 * 本案例展示了处理时间定时器
 */
public class Flink05_Timer_Process {
    public static void main(String[] args) throws Exception {
        // 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 从指定的网络端口上读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 8888);
        // 对读取的数据进行类型转换
        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(new WaterSensorMapFunction());

        SingleOutputStreamOperator<String> processDS = wsDS.process(
                new ProcessFunction<WaterSensor, String>() {
                    @Override
                    public void processElement(WaterSensor ws, ProcessFunction<WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        // 获取定时服务
                        TimerService timerService = ctx.timerService();

                        long currentProcessingTime = timerService.currentProcessingTime();
                        System.out.println("currentProcessingTime:" + currentProcessingTime);

                        // 注册处理时间定时器  定时器的触发是由系统时间触发的
                        timerService.registerProcessingTimeTimer(currentProcessingTime + 10000);
                    }

                    @Override
                    public void onTimer(long timestamp, ProcessFunction<WaterSensor, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("定时器在" + timestamp + "触发了");
                    }
                }
        );
        // 打印
        processDS.print();
        // 提交作业
        env.execute();
    }
}