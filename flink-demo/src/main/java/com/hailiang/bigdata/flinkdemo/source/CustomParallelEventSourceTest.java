package com.hailiang.bigdata.flinkdemo.source;

import com.hailiang.bigdata.flinkdemo.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CustomParallelEventSourceTest {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置全局并行度，方便控制台观察结果
        env.setParallelism(1);

        // 使用自定义的 CustomEventSource
        DataStreamSource<Event> eventDataStreamSource = env.addSource(new CustomParallelEventSource()).setParallelism(4);

        eventDataStreamSource.print();


        // 执行
        env.execute("Custom source");
    }
}
