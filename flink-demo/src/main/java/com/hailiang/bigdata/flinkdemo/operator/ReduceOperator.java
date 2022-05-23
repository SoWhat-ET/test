package com.hailiang.bigdata.flinkdemo.operator;

import com.hailiang.bigdata.flinkdemo.pojo.Event;
import com.hailiang.bigdata.flinkdemo.source.CustomParallelEventSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceOperator {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置全局并行度，方便控制台观察结果
        env.setParallelism(1);

        // 使用自定义的 CustomEventSource
        DataStreamSource<Event> eventDataStreamSource = env.addSource(new CustomParallelEventSource()).setParallelism(4);

        // 统计每个用户的访问次数
        // 调整数据结构为（用户名，1）
        SingleOutputStreamOperator<Tuple2<String, Long>> mapStream = eventDataStreamSource
                .map(event -> new Tuple2<String, Long>(event.user, 1L))
                // 显式指定返回的类型信息
                .returns(TupleTypeInfo.getBasicTupleTypeInfo(String.class, Long.class));

        //mapStream.print();

        // 按用户名分组
        mapStream.keyBy(tuple2 -> tuple2.f0)
                // reduce聚合
                .reduce((oldTuple2, newTuple2) -> new Tuple2<String, Long>(oldTuple2.f0, oldTuple2.f1 + newTuple2.f1))
                .print();

        env.execute();
    }
}
