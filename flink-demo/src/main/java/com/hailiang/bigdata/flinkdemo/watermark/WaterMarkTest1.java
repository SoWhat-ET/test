package com.hailiang.bigdata.flinkdemo.watermark;

import com.hailiang.bigdata.flinkdemo.pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class WaterMarkTest1 {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置全局并行度，方便控制台观察结果
        env.setParallelism(1);

        // 从 text file 中读取数据
        DataStreamSource<String> stringDataStreamSource = env.readTextFile("flink-demo/input/event.txt");



        // 事件时间单调递增时，指定单调递增的水位线生成策略
        SingleOutputStreamOperator<String> res1 = stringDataStreamSource.assignTimestampsAndWatermarks(WatermarkStrategy
                // 生成策略
                .<String>forMonotonousTimestamps()
                // 指定事件时间
                .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        String[] strings = element.split(",");
                        return Long.parseLong(strings[2]);
                    }
                })
        );

        // 事件时间为乱序时，指定最大延迟的水位线生成策略
        SingleOutputStreamOperator<String> res2 = stringDataStreamSource.assignTimestampsAndWatermarks(WatermarkStrategy
                // 指定生成策略和最大延迟时间
                .<String>forBoundedOutOfOrderness(Duration.ofMillis(200L))
                // 指定事件时间
                .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        String[] strings = element.split(",");
                        return Long.parseLong(strings[2]);
                    }
                })
        );





        // 执行
        env.execute("Read from text file");
    }

}
