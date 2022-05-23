package com.hailiang.bigdata.flinkdemo.operator;

import com.hailiang.bigdata.flinkdemo.pojo.Event;
import com.hailiang.bigdata.flinkdemo.source.CustomParallelEventSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FilterOperator {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置全局并行度，方便控制台观察结果
        env.setParallelism(1);

        // 使用自定义的 CustomEventSource
        DataStreamSource<Event> eventDataStreamSource = env.addSource(new CustomParallelEventSource()).setParallelism(4);


        // 过滤掉除 '徐大壮' 用户外的数据记录
        // 方法1: 传入 Lambda 表达式
        SingleOutputStreamOperator<Event> eventFilterStream = eventDataStreamSource.filter(event -> event.user.equals("徐大壮"));

        // 方法2：使用匿名类实现  FilterFunction 接口 略

        // 方法3：使用自定义类实现 FilterFunction 接口 略


        eventFilterStream.print();



        env.execute();
    }
}
