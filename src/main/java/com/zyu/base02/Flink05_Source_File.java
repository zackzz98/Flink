package com.zyu.base02;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.file.src.FileSource;

/**
 * @author zyu
 * date2025/3/6 17:08
 * 本案例展示了源算子 - 从文件中读取数据
 */
public class Flink05_Source_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从文件中读取数据
        // DataStreamSource<String> ds = env.readTextFile("D:\\my_study\\word.txt");

        FileSource<String> source = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(),
                new Path("D:\\my_study\\word.txt")
        ).build();
        // 封装为流 flink1.12 之后
        DataStreamSource<String> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "file_source");
        // 打印输出
        ds.print();
        // 提交作业
        env.execute();
    }
}