package com.zyu.base01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author zyu
 * date2025/2/28 15:00
 * 本案例展示了算子链
 */
public class OpeChain {
    public static void main(String[] args) throws Exception{
        //1. 环境准备
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);
        env.disableOperatorChaining(); // 全局禁用算子链
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
//                .disableChaining()
//                .startNewChain();
        //4. 对流中数据进行类型转换   String -> Tuple2<String, Long>
        SingleOutputStreamOperator<Tuple2<String, Long>> mapDS = flatMapDS.map(
                new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String word) throws Exception {
                        return Tuple2.of(word, 1L);
                    }
                }
        );
        // 5. 按照单词进行分组求和
        KeyedStream<Tuple2<String, Long>, Tuple> keyedDS = mapDS.keyBy(0);
        SingleOutputStreamOperator<Tuple2<String, Long>> sumDS = keyedDS.sum(1);
        //6. 打印结果并提交
        sumDS.print();
        env.execute();
    }
}