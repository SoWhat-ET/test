package com.hailiang.bigdata.flinkdemo.operator;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CustomRichFunction {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置全局并行度，方便控制台观察结果
        env.setParallelism(1);

        // 从 text file 中读取数据
        DataStreamSource<String> stringDataStreamSource = env.readTextFile("flink-demo/input/event.txt");

        stringDataStreamSource.map(new MyRichMap()).print();

        env.execute();
    }

    public static class MyRichMap extends RichMapFunction<String,String>{

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("Open方法被调用:"+getRuntimeContext().getIndexOfThisSubtask() + "号任务启动");
        }

        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("Close方法被调用:"+getRuntimeContext().getIndexOfThisSubtask() + "号任务结束");
        }


        @Override
        public String map(String value) throws Exception {
            String[] strings = value.split(",");
            return strings[0];
        }
    }
}
