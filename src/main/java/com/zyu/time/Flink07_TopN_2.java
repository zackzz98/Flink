package com.zyu.time;

import com.zyu.bean.WaterSensor;
import com.zyu.func.MySourceFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zyu
 * date2025/3/25 10:01
 * 本案例展示了TopN实现的第二种方式
 */
public class Flink07_TopN_2 {
    public static void main(String[] args) throws Exception {
        // 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 使用自定义数据源读取数据
        DataStreamSource<WaterSensor> customDS = env.addSource(new MySourceFunction());
        // 指定Watermark的生成策略以及提取事件时间字段
        SingleOutputStreamOperator<WaterSensor> watermarkDS = customDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<WaterSensor>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<WaterSensor>() {
                                    @Override
                                    public long extractTimestamp(WaterSensor ws, long recordTimestamp) {
                                        return ws.getTs() * 1000;
                                    }
                                }
                        )
        );
        // TODO 按照水位值进行分组
        KeyedStream<WaterSensor, Integer> keyedDS = watermarkDS.keyBy(WaterSensor::getVc);

        // TODO 开窗 WaterSensor  滑动事件时间窗口
        WindowedStream<WaterSensor, Integer, TimeWindow> windowedDS = keyedDS.window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)));

        // TODO 对窗口中的数据进行处理  Tuple3<Vc, Count, End>
        SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> aggregateDS = windowedDS.aggregate(
                new AggregateFunction<WaterSensor, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(WaterSensor ws, Integer accumulator) {
                        return ++accumulator;
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return null;
                    }
                },
                new ProcessWindowFunction<Integer, Tuple3<Integer, Integer, Long>, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer vc, ProcessWindowFunction<Integer, Tuple3<Integer, Integer, Long>, Integer, TimeWindow>.Context context, Iterable<Integer> elements, Collector<Tuple3<Integer, Integer, Long>> out) throws Exception {
                        Integer count = elements.iterator().next();
                        long end = context.window().getEnd();
                        out.collect(Tuple3.of(vc, count, end));
                    }
                }
        );
        // TODO 再次按照窗口的结束时间进行分组(将相同窗口的数据放到一组进行处理)
        KeyedStream<Tuple3<Integer, Integer, Long>, Long> endKeyedDS = aggregateDS.keyBy(tup3 -> tup3.f2);

        // TODO 对分组后的数据进行处理 -- process
        SingleOutputStreamOperator<String> processDS = endKeyedDS.process(
                new KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String>() {

                    Map<Long, List<Tuple3<Integer, Integer, Long>>> vcCountEndMap = new HashMap<>();

                    @Override
                    public void processElement(Tuple3<Integer, Integer, Long> tup3, KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String>.Context ctx, Collector<String> out) throws Exception {
                        Long end = tup3.f2;
                        if (vcCountEndMap.containsKey(end)) {
                            vcCountEndMap.get(end).add(tup3);
                        } else {
                            List<Tuple3<Integer, Integer, Long>> vcCountEndList = new ArrayList<>();
                            vcCountEndList.add(tup3);
                            vcCountEndMap.put(end, vcCountEndList);
                        }
                        // TODO 注意：为了保证同一个窗口的数据都到达后再去进行排序取TopN
                        // TODO 注册一个事件时间定时器
                        TimerService timerService = ctx.timerService();
                        timerService.registerEventTimeTimer(end + 1);
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        // 定时器被触发的时候执行的方法
                        long end = ctx.getCurrentKey();
                        List<Tuple3<Integer, Integer, Long>> vcCountEndList = vcCountEndMap.get(end);
                        vcCountEndList.sort((o1, o2) -> o2.f1 - o1.f1);

                        // 取TopN
                        StringBuilder outStr = new StringBuilder();

                        outStr.append("========================\n");
                        // 遍历， 排序后的List， 取出前threshold个， 考虑可能List不够两个的情况 =》 List中元素的个数和2取最小值
                        for (int i = 0; i < Math.min(2, vcCountEndList.size()); i++) {
                            Tuple3<Integer, Integer, Long> vcCount = vcCountEndList.get(i);
                            outStr.append("Top" + (i + 1) + "\n");
                            outStr.append("vc=" + vcCount.f0 + "\n");
                            outStr.append("count=" + vcCount.f1 + "\n");
                            outStr.append("窗口结束时间=" + vcCount.f2 + "\n");
                            outStr.append("================================\n");
                        }
                        // 用完的List, 及时清理， 节省资源
                        vcCountEndList.clear();
                        out.collect(outStr.toString());
                    }
                }
        );
        // 打印
        processDS.print();
        // 提交作业
        env.execute();
    }
}