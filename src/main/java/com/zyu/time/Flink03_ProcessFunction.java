package com.zyu.time;

import com.zyu.bean.WaterSensor;
import com.zyu.func.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zyu
 * date2025/3/24 13:46
 * 本案例展示了处理函数分分类
 * TODO
 * 1. map函数：用于将WaterSensor对象转换为字符串
 * 2. ProcessFunction：该函数允许对每个输入元素进行处理, 并可以访问上下文信息
 * 3. KeyedProcessFunction：用于处理按键分组的流, 允许对每个键的状态进行管理
 * 4. ProcessAllWindowFunction和ProcessWindowFunction:用于处理窗口中的所有元素, 分别适用于全局窗口和按键窗口
 * 5. CoProcessFunction: 用于处理两个流的连接, 允许对两个流中的元素进行处理
 * 6. BroadcastProcessFunction: 用于处理广播流, 允许对广播状态进行管理
 */
public class Flink03_ProcessFunction {
    public static void main(String[] args) throws Exception {
        // 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从指定的网络端口读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 8888);
        // 对读取的数据进行类型转换
        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(new WaterSensorMapFunction());

        // TODO 处理函数的使用
        /*wsDS.map(
                new MapFunction<WaterSensor, String>() {
                    @Override
                    public String map(WaterSensor waterSensor) throws Exception {
                        return "";
                    }
                }
        )*/
        /*wsDS.process(
                new ProcessFunction<WaterSensor, String>() {
                    @Override
                    public void processElement(WaterSensor value, ProcessFunction<WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {

                    }
                }
        )
        wsDS
                .keyBy(WaterSensor::getId)
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {
                            @Override
                            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {

                            }
                        }
                )

        wsDS
                .windowAll()
                .process(
                        new ProcessAllWindowFunction<WaterSensor, Object, Window>() {
                            @Override
                            public void process(ProcessAllWindowFunction<WaterSensor, Object, Window>.Context context, Iterable<WaterSensor> elements, Collector<Object> out) throws Exception {

                            }
                        }
                )

        wsDS
                .keyBy()
                .window()
                .process(
                        new ProcessWindowFunction<WaterSensor, Object, Tuple, Window>() {
                            @Override
                            public void process(Tuple tuple, ProcessWindowFunction<WaterSensor, Object, Tuple, Window>.Context context, Iterable<WaterSensor> elements, Collector<Object> out) throws Exception {

                            }
                        }
                )

        wsDS
                .connect(wsDS)
                .process(
                        new CoProcessFunction<WaterSensor, WaterSensor, Object>() {
                            @Override
                            public void processElement1(WaterSensor value, CoProcessFunction<WaterSensor, WaterSensor, Object>.Context ctx, Collector<Object> out) throws Exception {

                            }

                            @Override
                            public void processElement2(WaterSensor value, CoProcessFunction<WaterSensor, WaterSensor, Object>.Context ctx, Collector<Object> out) throws Exception {

                            }
                        }
                )

        wsDS
                .keyBy()
                .intervalJoin()
                .between()
                .process(
                        new ProcessJoinFunction<WaterSensor, Object, Object>() {
                            @Override
                            public void processElement(WaterSensor left, Object right, ProcessJoinFunction<WaterSensor, Object, Object>.Context ctx, Collector<Object> out) throws Exception {

                            }
                        }
                )

        wsDS.connect(wsDS.broadcast(new MapStateDescriptor<Object, Object>()))
                .process(
                        new BroadcastProcessFunction<WaterSensor, WaterSensor, Object>() {
                            @Override
                            public void processElement(WaterSensor value, BroadcastProcessFunction<WaterSensor, WaterSensor, Object>.ReadOnlyContext ctx, Collector<Object> out) throws Exception {

                            }

                            @Override
                            public void processBroadcastElement(WaterSensor value, BroadcastProcessFunction<WaterSensor, WaterSensor, Object>.Context ctx, Collector<Object> out) throws Exception {

                            }
                        }
                )

         */
    }
}