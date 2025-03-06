package com.zyu.wordCount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.util.Collector;

/**
 * @author zyu
 * date2025/2/26 18:13
 * 以流的形式对无界数据进行处理
 */
public class UnBoundStreamGeneric {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .socketTextStream("bg01", 7777)
                .flatMap(
                        (String lineStr, Collector<Tuple2<String, Long>> out) -> {
                            String[] wordArr = lineStr.split(" ");
                            for (String word : wordArr) {
                                out.collect(Tuple2.of(word, 1L));
                            }
                        }
                )
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(0)  // word -> word.f0 这样写？
                .sum(1)
                .print();
        // 提交作业
        env.execute();
    }
}