package com.zyu.sink;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zyu
 * date2025/3/13 14:59
 * 本案例展示了分区算子
 * 分区算子
 *  shuffle
 *  rebalance
 *  rescale
 *  broadcast
 *  global
 *  keyby
 *  forward
 *  custom
 */
public class Flink02_Par {
    public static void main(String[] args) throws Exception {
        // 环境准备
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(10);
        // 从指定的网络端口读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("bg01", 8888);

        //TODO shuffle
//        socketDS.shuffle().print();
        //TODO rebalance
//        socketDS.map(a->a).setParallelism(2).rebalance().print();
        //TODO rescale
//        socketDS.map(a->a).setParallelism(2).rescale().print();
        //TODO broadcast
//        socketDS.broadcast().print();
        //TODO global
//        socketDS.global().print();
        //TODO keyby
//        socketDS.keyBy(a->a).print();
        //TODO forward
//        socketDS.print();
        //TODO 自定义分区器
        socketDS.partitionCustom(
                new MyPartitioner(),
                a->a
        ).print();

        env.execute();
    }
}

class MyPartitioner implements Partitioner<String> {
    @Override
    public int partition(String key, int numPartitions) {
        return key.hashCode()%numPartitions;
    }
}