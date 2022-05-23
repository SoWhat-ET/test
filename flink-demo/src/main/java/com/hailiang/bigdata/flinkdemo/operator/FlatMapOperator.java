package com.hailiang.bigdata.flinkdemo.operator;

import com.hailiang.bigdata.flinkdemo.pojo.Event;
import com.hailiang.bigdata.flinkdemo.source.CustomParallelEventSource;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class FlatMapOperator {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置全局并行度，方便控制台观察结果
        env.setParallelism(1);

        // 使用自定义的 CustomEventSource
        DataStreamSource<Event> eventDataStreamSource = env.addSource(new CustomParallelEventSource()).setParallelism(4);


        // 过滤掉非 "徐大壮" 记录的数据，拆分 Event 的所有字段（Flat压平）每个字段拼上 "金毛" 输出（Map转换）
        // 方法1: 传入 Lambda 表达式
        eventDataStreamSource.flatMap((Event event, Collector<String> out) -> {
            if (event.user.equals("徐大壮")) {
                out.collect("金毛");
                out.collect(event.user);
                out.collect(event.url);
                out.collect(event.timestamp.toString());
            }
//                    }else {
//                        out.collect(event.user);
//                    }
        }).returns(BasicTypeInfo.STRING_TYPE_INFO).print();

        // 方法2：使用匿名类实现  FilterFunction 接口 略

        // 方法3：使用自定义类实现 FilterFunction 接口 略


        env.execute();
    }
}
