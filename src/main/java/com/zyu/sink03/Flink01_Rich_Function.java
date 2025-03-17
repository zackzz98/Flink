package com.zyu.sink03;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

/**
 * @author zyu
 * date2025/3/12 16:23
 * 本案例展示了富函数类
 * 需求：从指定的网络端口读取数据，给读取的每条数据加"zyu-"前缀再向下游传递
 * 富函数类-RichFunction
 *      当对流中数据进行处理的时候，处理算子要求传递一个处理对象作为参数，在声明参数的时候，其形式是接口的形式
 *      例如：map-MapFunction  filter-FilterFunction  flatMap-FlatMapFunction
 *      当一个处理函数接口都有一个对应的实现类，命名  Rich + 接口名称
 *      例如：map-RichMapFunction  filter-RichFilterFunction  flatMap-RichFlatMapFunction
 *      这样的类我们称之为富函数类
 *
 *      富函数类除了具备普通除了函数的功能外，还多了如下功能
 *          可以通过上下文对象获取更丰富的信息
 *          富函数具有生命周期方法 以map算子为例
 *          open
 *              在算子进行初始化的执行的方法，一般用于连接初始化，每一个并行度（算子任务）上，执行一次
 *          close
 *              在算子运行结束的执行的方法，一般用于释放资源，每一个并行度（算子子任务）上，执行一次
 *              注意：如果我们处理的是无界数据，程序不会停止，close方法永远不会被调用
 *          map
 *              流中每来一条数据，都会执行
 *
 */
public class Flink01_Rich_Function {
    public static void main(String[] args) throws Exception {
        // 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        // 从指定的网络端口读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("bg01", 8888);

/*        SingleOutputStreamOperator<String> mapDS = socketDS.map(
                new RichMapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        return "zyu-" + value;
                    }
                }
        );*/

        SingleOutputStreamOperator<String> mapDS = socketDS.map(
                new RichMapFunction<String, String>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        System.out.println("== open ==");
                        int index = getRuntimeContext().getIndexOfThisSubtask();
                        String name = getRuntimeContext().getTaskNameWithSubtasks();
                        System.out.println("索引号是"+index+"的子任务["+name+"]正在初始化");
                    }
                    @Override
                    public void close() throws Exception {
                        System.out.println("== close ==");
                    }
                    @Override
                    public String map(String value) throws Exception {
                        return "zyu-" + value;
                    }
                }
        );

        // 打印
        mapDS.print();
        // 提交作业
        env.execute();
    }
}