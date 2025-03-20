package com.zyu.base;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * @author zyu
 * date2025/3/6 15:06
 * 本案例展示了任务槽
 */
public class Slot {
    public static void main(String[] args) throws Exception{
        //1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2. 从指定的网络端口中读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("bg01", 7777);
        //3. 对读取的数据进行扁平化处理
        SingleOutputStreamOperator<String> flatMapDS = socketDS.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String lineStr, Collector<String> out) throws Exception {
                        String[] wordArr = lineStr.split(" ");
                        for (String word : wordArr) {
                            out.collect(word);
                        }
                    }
                }
        );
        //4. 对流中数据进行类型转换   String -> Tuple2<String, Long>
        SingleOutputStreamOperator<Tuple2<String, Long>> mapDS = flatMapDS.map(
                new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String word) throws Exception {
                        return Tuple2.of(word, 1L);
                    }
                }
        ).slotSharingGroup("default");
        //5. 按照单词进行分组求和
        KeyedStream<Tuple2<String, Long>, Tuple> keyedDS = mapDS.keyBy(0);
        SingleOutputStreamOperator<Tuple2<String, Long>> sumDS = keyedDS.sum(1);
        //6. 打印结果并提交作业
        sumDS.print();
        env.execute();
    }
}