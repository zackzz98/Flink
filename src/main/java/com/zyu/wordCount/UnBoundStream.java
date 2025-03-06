package com.zyu.wordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author zyu
 * date2025/2/26 17:45
 * 以流的形式对无界数据进行处理
 */
public class UnBoundStream {
    public static void main(String[] args) throws Exception {
        //1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2. 从指定的网络端口读取数据  采用本地localhost 7777  ncat -lk 7777
        DataStreamSource<String> socketDS = env.socketTextStream("bg01", 7777);
        //3. 对读取的数据进行扁平化处理      封装为二元组对象向下游传递 Tuple2<String,Long>
        SingleOutputStreamOperator<Tuple2<String, Long>> flatMapDS = socketDS.flatMap(
                new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String lineStr, Collector<Tuple2<String, Long>> out) throws Exception {
                        String[] wordArr = lineStr.split(" ");
                        for (String word : wordArr) {
                            out.collect(Tuple2.of(word, 1L));
                        }
                    }
                }
        );
        //4. 按照单词进行分组
        KeyedStream<Tuple2<String, Long>, Tuple> keyedDS = flatMapDS.keyBy(0);
        //5. 求和计算
        SingleOutputStreamOperator<Tuple2<String, Long>> sumDS = keyedDS.sum(1);
        //6. 将结果打印输出
        sumDS.print();
        //7. 提交作业
        env.execute();
    }
}