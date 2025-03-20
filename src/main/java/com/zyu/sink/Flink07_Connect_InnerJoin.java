package com.zyu.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
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
                Tuple2.of(1, "a2"),
                Tuple2.of(3, "c3"),
                Tuple2.of(4, "d4")
        );
        DataStreamSource<Tuple3<Integer, String, Integer>> ds2 = env.fromElements(
                Tuple3.of(1, "aa1", 10),
                Tuple3.of(1, "aa2", 9),
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
                    // 用于缓存第一条流中的元素
                    Map<Integer,List<Tuple2<Integer,String>>> ds1Cache = new HashMap<>();
                    // 用于缓存第二条流中的元素
                    Map<Integer, List<Tuple3<Integer, String, Integer>>> ds2Cache = new HashMap<>();

                    @Override
                    public void processElement1(Tuple2<Integer, String> tup2, KeyedCoProcessFunction<Integer, Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                        Integer id = tup2.f0;
                        // 将当前数据放到缓存中缓存起来
                        if (ds1Cache.containsKey(id)) {
                            ds1Cache.get(id).add(tup2);
                        } else {
                            List<Tuple2<Integer, String>> ds1List = new ArrayList<>();
                            ds1List.add(tup2);
                            ds1Cache.put(id, ds1List);
                        }
                        // 用当前这条数据和另外一条流已经缓存的数据进行关联
                        if (ds2Cache.containsKey(id)) {
                            for (Tuple3<Integer, String, Integer> tup3 : ds2Cache.get(id)) {
                                out.collect(tup2 + "------" + tup3);
                            }
                        }
                    }

                    @Override
                    public void processElement2(Tuple3<Integer, String, Integer> tup3, KeyedCoProcessFunction<Integer, Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                        Integer id = tup3.f0;
                        // 将当前数据放到缓存中缓存起来
                        if (ds2Cache.containsKey(id)) {
                            ds2Cache.get(id).add(tup3);
                        } else {
                            List<Tuple3<Integer, String, Integer>> ds2List = new ArrayList<>();
                            ds2List.add(tup3);
                            ds2Cache.put(id, ds2List);
                        }
                        // 用当前这条数据和另外一条流缓存的数据进行关联
                        if (ds1Cache.containsKey(id)) {
                            for (Tuple2<Integer, String> tup2 : ds1Cache.get(id)) {
                                out.collect(tup2 + "=======" + tup3);
                            }
                        }
                    }
                }
        );
        // 打印
        processDS.print();
        // 提交作业
        env.execute();

        // 输出结果

/*      (1,a1)------(1,aa1,10)
        (1,a1)=======(1,aa2,9)
        (1,a2)------(1,aa1,10)
        (1,a2)------(1,aa2,9)
        (3,c3)------(3,cc3,8)
        (4,d4)------(4,dd4,7)*/
    }
}