package com.hailiang.bigdata.demo1.db;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;


public class Gmall2Kafka {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("10.30.5.70")
                .port(3306)
                .databaseList("gmall") // set captured database
                .tableList("gmall.*") // set captured table
                .username("root")
                .password("Bigdata.dev.123")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        // enable checkpoint
        env.enableCheckpointing(3000);

        DataStreamSource<String> mySQLSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .setParallelism(4);

        //mySQLSource.print().setParallelism(1); // use parallelism 1 for sink to keep message ordering


        //将数据写入Kafka
        mySQLSource.addSink(new FlinkKafkaProducer<String>("10.30.5.68:9092,10.30.5.69:9092,10.30.5.70:9092","gmall",new SimpleStringSchema()))
                .setParallelism(1);

        //执行
        env.execute("Gmall2Kafka");

    }
}
