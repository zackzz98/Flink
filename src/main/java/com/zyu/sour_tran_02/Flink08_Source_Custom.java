package com.zyu.sour_tran_02;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author zyu
 * date2025/3/11 14:21
 * 本案例展示了源算子 - 自定义数据源
 */
public class Flink08_Source_Custom {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // 从数据源获取数据
        DataStreamSource<String> ds = env.addSource(new MySource());
        // 打印输出
        ds.print();
        // 提交作业
        env.execute();
    }
}

class MySource implements SourceFunction<String> {
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        for (int i = 0; i < 100; i++) {
            //将数据向下游传递
            ctx.collect("数据 -> " + i);
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        System.out.println(" ");
    }

}