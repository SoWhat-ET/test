package com.hailiang.bigdata.flinkdemo.operator;

import com.hailiang.bigdata.flinkdemo.pojo.Event;
import com.hailiang.bigdata.flinkdemo.source.CustomParallelEventSource;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class KeyByOperator {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置全局并行度，方便控制台观察结果
        env.setParallelism(1);

        // 使用自定义的 CustomEventSource
        DataStreamSource<Event> eventDataStreamSource = env.addSource(new CustomParallelEventSource()).setParallelism(4);


        // 方法1: 传入 Lambda 表达式
        // maxBy 更新maxBy字段及其他字段的值
        eventDataStreamSource.keyBy(event -> event.user)
                .maxBy("timestamp")
                .print("maxBy:");



        // 方法2：使用匿名类实现
        // max 只更新max字段的值
        eventDataStreamSource.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event value) throws Exception {
                return value.user;
            }
        })
                .max("timestamp")
                .print("max:");

        // 方法3：使用自定义类实现 略


        env.execute();
    }
}
