package com.zyu.base03;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * @author zyu
 * date2025/3/14 16:08
 * 本案例展示了通过connect算子实现两条流内连接效果
 */
public class Flink07_Connect_InnerJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 准备流中数据
        DataStreamSource<Tuple2<Integer, String>> ds1 = env.fromElements(
                Tuple2.of(1, "a1"),
                Tuple2.of(2, "b2"),
                Tuple2.of(3, "c3"),
                Tuple2.of(4, "d4")
        );
        DataStreamSource<Tuple3<Integer, String, Integer>> ds2 = env.fromElements(
                Tuple3.of(1, "aa1", 10),
                Tuple3.of(2, "bb2", 9),
                Tuple3.of(3, "cc3", 8),
                Tuple3.of(4, "dd4", 7)
        );

        // 使用connect进行合流
        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> connectDS = ds1.connect(ds2);
        // 按照id进行分组
        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> keyedDS = connectDS.keyBy(
                tup2 -> tup2.f0,
                tup3 -> tup3.f0
        );

        // 对连接后数据进行处理
        SingleOutputStreamOperator<String> processDS = keyedDS.process(
                new KeyedCoProcessFunction<Integer, Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>() {
                    @Override
                    public void processElement1(Tuple2<Integer, String> integerStringTuple2, KeyedCoProcessFunction<Integer, Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>.Context context, Collector<String> collector) throws Exception {
                        // 缓存第一条流中的元素
//                        Map<Integer, List>

                    }

                    @Override
                    public void processElement2(Tuple3<Integer, String, Integer> integerStringIntegerTuple3, KeyedCoProcessFunction<Integer, Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>.Context context, Collector<String> collector) throws Exception {

                    }
                }
        );
    }
}