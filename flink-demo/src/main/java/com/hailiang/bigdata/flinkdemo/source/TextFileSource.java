package com.hailiang.bigdata.flinkdemo.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TextFileSource {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置全局并行度，方便控制台观察结果
        env.setParallelism(1);

        // 从 text file 中读取数据
        DataStreamSource<String> stringDataStreamSource = env.readTextFile("flink-demo/input/event.txt");

        stringDataStreamSource.print();


        // 执行
        env.execute("Read from text file");
    }


}
