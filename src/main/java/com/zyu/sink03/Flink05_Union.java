package com.zyu.sink03;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zyu
 * date2025/3/13 17:51
 * 本案例展示了合流算子 - union
 *      union可以合并两条或者多条流
 *      参与合并的流中数据类型必须要一致
 */
public class Flink05_Union {
    public static void main(String[] args) throws Exception {
        // 准备环境
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 从指定的网络端口读取数据
        DataStreamSource<String> ds1 = env.socketTextStream("bg01",8888);
        DataStreamSource<String> ds2 = env.socketTextStream("bg01",8889);
        DataStreamSource<Integer> ds3 = env.fromElements(1, 1, 2, 4);
        // 使用union合流
        DataStream<String> unionDS = ds1.union(ds2);
        // 打印
        unionDS.print();
        // 提交作业
        env.execute();
    }
}