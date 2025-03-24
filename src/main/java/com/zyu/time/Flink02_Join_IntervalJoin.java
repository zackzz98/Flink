package com.zyu.time;

import com.zyu.bean.Dept;
import com.zyu.bean.Emp;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author zyu
 * date2025/3/24 10:33
 * 本案例展示了IntervalJoin
 */
public class Flink02_Join_IntervalJoin {
    public static void main(String[] args) throws Exception {
        // 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从指定的网络端口读取员工数据 并指定Watermark的生成策略以及提取事件时间字段
        SingleOutputStreamOperator<Emp> empDS = env
                .socketTextStream("localhost", 8888)
                .map(
                        new MapFunction<String, Emp>() {
                            @Override
                            public Emp map(String lineStr) throws Exception {
                                String[] fieldArr = lineStr.split(",");
                                return new Emp(Integer.valueOf(fieldArr[0]), fieldArr[1], Integer.valueOf(fieldArr[2]), Long.valueOf(fieldArr[3]));
                            }
                        }
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Emp>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Emp>() {
                                            @Override
                                            public long extractTimestamp(Emp emp, long recordTimestamp) {
                                                return emp.getTs();
                                            }
                                        }
                                )
                );
        empDS.print("Emp:");
        // 从指定的网络端口中读取部门数据 并指定Watermark的生成策略以及提取事件时间字段
        SingleOutputStreamOperator<Dept> deptDS = env
                .socketTextStream("localhost", 8887)
                .map(
                        new MapFunction<String, Dept>() {
                            @Override
                            public Dept map(String lineStr) throws Exception {
                                String[] fieldArr = lineStr.split(",");
                                return new Dept(Integer.valueOf(fieldArr[0]), fieldArr[1], Long.valueOf(fieldArr[2]));
                            }
                        }
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Dept>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Dept>() {
                                            @Override
                                            public long extractTimestamp(Dept dept, long recordTimestamp) {
                                                return dept.getTs();
                                            }
                                        }
                                )
                );
        deptDS.print("Dept:");
        // 使用IntervalJoin进行关联
        SingleOutputStreamOperator<Tuple2<Emp, Dept>> joinedDS = empDS
                .keyBy(Emp::getDeptno)
                .intervalJoin(deptDS.keyBy(Dept::getDeptno))
                .between(Time.milliseconds(-5), Time.milliseconds(5))
                .process(
                        new ProcessJoinFunction<Emp, Dept, Tuple2<Emp, Dept>>() {
                            @Override
                            public void processElement(Emp emp, Dept dept, ProcessJoinFunction<Emp, Dept, Tuple2<Emp, Dept>>.Context ctx, Collector<Tuple2<Emp, Dept>> out) throws Exception {
                                out.collect(Tuple2.of(emp, dept));
                            }
                        }
                );
        // 打印
        joinedDS.print();
        // 提交作业
        env.execute();
    }
}