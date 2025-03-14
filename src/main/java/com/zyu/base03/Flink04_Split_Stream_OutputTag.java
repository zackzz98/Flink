package com.zyu.base03;

import com.zyu.bean.WaterSensor;
import com.zyu.func.WaterSensorMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author zyu
 * date2025/3/13 17:09
 * 本案例展示了通过侧输出流实现分流
 * 注意：如果使用侧输出流，必须使用process算子对流中数据进行处理，因为只有process底层的processElement方法中提供了上下文对象 ctx.output();
 *
 * 需求：将WaterSensor按照Id类型进行分流
 */
public class Flink04_Split_Stream_OutputTag {
    public static void main(String[] args) throws Exception {
        // 环境准备
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 从指定的网络端口读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("bg01", 8888);
        // 对流中数据进行类型转换   String -> WaterSensor
        SingleOutputStreamOperator<WaterSensor> wsDS = socketDS.map(new WaterSensorMapFunction());
        //todo 分流
        //todo 创建侧输出流标签
        OutputTag<WaterSensor> ws1Tag = new OutputTag<WaterSensor>("ws1"){};
        OutputTag<WaterSensor> ws2Tag = new OutputTag<WaterSensor>("ws2", Types.POJO(WaterSensor.class));
        //todo 分流逻辑
        SingleOutputStreamOperator<WaterSensor> mainDS = wsDS.process(
                new ProcessFunction<WaterSensor, WaterSensor>() {
                    @Override
                    public void processElement(WaterSensor ws, ProcessFunction<WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                        String id = ws.getId();
                        if ("ws1".equals(id)){
                            // 将数据“放到”侧输出流中（给这条数据打标签）
                            ctx.output(ws1Tag, ws);
                        } else if ("ws2".equals(id)) {
                            ctx.output(ws2Tag, ws);
                        } else {
                            // 将数据放到主流
                            out.collect(ws);
                        }
                    }
                }
        );
        // 打印
        mainDS.print("主流：");
        mainDS.getSideOutput(ws1Tag).print("ws1:");
        mainDS.getSideOutput(ws2Tag).print("ws2:");
        // 提交作业
        env.execute();
    }
}