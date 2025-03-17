package com.zyu.sink03;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zyu
 * date2025/3/13 15:44
 * 本案例展示了通过filter算子分流
 * 需求：读取一个整数数字流，将数据流分为奇数流和偶数流
 */
public class Flink03_Split_Stream_Filter {
    public static void main(String[] args) throws Exception {
        // 环境准备
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        // 从指定的网络端口读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("bg01", 8888);
        // 对流中数据进行类型转换   String -> Integer  依次输入 1 2 3 4 5 可测试
        SingleOutputStreamOperator<Integer> mapDS = socketDS.map(Integer::valueOf);

        // 也可以使用这种方式实现 1,2,3,4,5 这种数据测试
/*        SingleOutputStreamOperator<Integer> mapDS = socketDS.flatMap(new FlatMapFunction<String, Integer>() {
            @Override
            public void flatMap(String value, Collector<Integer> out) throws Exception {
                String[] numbers = value.split(","); // 拆分逗号分隔的字符串

                for (String number : numbers) {
                    out.collect(Integer.valueOf(number.trim())); // 转换为整数并输出
                }
            }
        }).setParallelism(1);*/


        // 过滤出奇数流
        SingleOutputStreamOperator<Integer> ds1 = mapDS.filter(num -> num % 2 != 0).setParallelism(2);
        // 过滤出偶数流
        SingleOutputStreamOperator<Integer> ds2 = mapDS.filter(num -> num % 2 == 0).setParallelism(3);
        // 打印输出
        ds1.print("奇数：");
        ds2.print("偶数：");
        // 提交作业
        env.execute();
    }
}