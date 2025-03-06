package com.zyu.wordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author zyu
 * date2025/2/26 16:49
 * 本案例展示以批的形式处理有界数据
 */
public class BoundBatch {
    public static void main(String[] args) {
        //1. 环境准备
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2. 从指定的文件中读取数据
        DataSource<String> ds = env.readTextFile("input/words.txt");
        //3. 对读取的数据进行扁平化处理      封装为二元组对象 Tuple2<单词,1L>
        FlatMapOperator<String, Tuple2<String, Long>> flatMapDS = ds.flatMap(
                new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String lineStr, Collector<Tuple2<String, Long>> out) throws Exception {
                        String[] wordArr = lineStr.split(" ");
                        for (String word : wordArr) {
                            //将封装好的二元组对象发送到下游
                            out.collect(Tuple2.of(word, 1L));
                        }
                    }
                }
        );
        //4. 按照单词进行分组
        UnsortedGrouping<Tuple2<String, Long>> groupByDS = flatMapDS.groupBy(0);
        //5. 聚合计算
        AggregateOperator<Tuple2<String, Long>> sumDS = groupByDS.sum(1);
        //6. 将结果打印输出
        try {
            sumDS.print();
            // 输出结果为:
            //(flink1.17,1)
            //(world,1)
            //(hello,3)
            //(zack,1)
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
