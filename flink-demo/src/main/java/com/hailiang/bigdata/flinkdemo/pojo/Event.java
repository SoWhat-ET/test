package com.hailiang.bigdata.flinkdemo.pojo;


import java.sql.Timestamp;

/*  定义POJO类型的类 Event
 *     Flink中POJO类型需满足：
 *         1、是公共类
 *         2、无参构造是公共的
 *         3、所有属性是可获得的（公共属性或提供 get set 方法）
 *         4、字段类型必须是Flink支持的.（Flink能够用Avro来序列化）
 *  Flink访问POJO类型效率更高
 */
public class Event {
    public String user;
    public String url;
    public Long timestamp;

    public Event() {
    }

    public Event(String user, String url, Long timestamp) {


        this.user = user;
        this.url = url;
        this.timestamp = timestamp;
    }

    /**
     * 重写 toString 方法，方便测试时显示更为清晰
     * @return
     */
    @Override
    public String toString() {
        return "Event{" +
                "user = '" + user + '\'' +
                ", url = " + url + '\'' +
                ", timestamp = " + new Timestamp(timestamp) +
                "}";
    }
}
