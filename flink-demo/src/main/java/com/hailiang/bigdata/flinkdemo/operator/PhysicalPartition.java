package com.hailiang.bigdata.flinkdemo.operator;


import com.hailiang.bigdata.flinkdemo.pojo.Event;
import com.hailiang.bigdata.flinkdemo.source.CustomEventSource;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// KeyBy 算子是按照Key的哈希值进行分区标记（逻辑上的）物理上没有进行分区
public class PhysicalPartition {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置全局并行度，方便控制台观察结果
        env.setParallelism(1);

        // 使用自定义的 CustomEventSource
        DataStreamSource<Event> eventDataStreamSource = env.addSource(new CustomEventSource());

        // 对数据进行物理分区
        // shuffle 随机分区
        //eventDataStreamSource.shuffle().print().setParallelism(4);

        // rebalance 轮询分区(若并行度发生改变，会默认自动调用轮询分区的方式来分发数据)
        //eventDataStreamSource.rebalance().print().setParallelism(4);

        // rescale 重缩放分区,对上游数据按并行度分组，组内执行轮询分区
        //eventDataStreamSource.rescale().print().setParallelism(4);

        // 全局分区 把所有数据都发送到一个分区
        //eventDataStreamSource.global().print().setParallelism(4);

        // 自定义重分区
        DataStreamSource<Integer> integerDataStreamSource = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9);
        // partitionCustom中传入自定义的分区器Partitioner和键选择器KeySelector
        integerDataStreamSource.partitionCustom(new Partitioner<Integer>() {
            @Override
            public int partition(Integer key, int numPartitions) {
                // 按照key的奇偶性分区
                return key % 2;
            }
        }, new KeySelector<Integer, Integer>() {
            @Override
            public Integer getKey(Integer value) throws Exception {
                return value;
            }
        }).print().setParallelism(4);


        env.execute();
    }

}
