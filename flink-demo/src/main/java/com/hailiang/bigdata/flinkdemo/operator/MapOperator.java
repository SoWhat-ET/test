package com.hailiang.bigdata.flinkdemo.operator;

import com.hailiang.bigdata.flinkdemo.pojo.Event;
import com.hailiang.bigdata.flinkdemo.source.CustomParallelEventSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapOperator{
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置全局并行度，方便控制台观察结果
        env.setParallelism(1);

        // 使用自定义的 CustomEventSource
        DataStreamSource<Event> eventDataStreamSource = env.addSource(new CustomParallelEventSource()).setParallelism(4);

        // 需求：提取数据流中的人名
        // 方法1：使用匿名类实现 MapFunction 接口
        SingleOutputStreamOperator<String> nameStream1 = eventDataStreamSource.map(new MapFunction<Event, String>() {
            public String map(Event value) throws Exception {
                return value.user;
            }
        });

        // 方法2：传入 Lambda 表达式
        SingleOutputStreamOperator<String> nameStream2 = eventDataStreamSource.map(event -> event.user);

        // 方法3：使用自定义类实现 MapFunction 接口
        SingleOutputStreamOperator<String> nameStream3 = eventDataStreamSource.map(new MyMapFunction());

        nameStream1.print("方法1：");
        nameStream2.print("方法2：");
        nameStream3.print("方法3：");

        env.execute();
    }

    public static class MyMapFunction implements MapFunction<Event,String>{
        public String map(Event value) throws Exception {
            return value.user;
        }
    }

}
