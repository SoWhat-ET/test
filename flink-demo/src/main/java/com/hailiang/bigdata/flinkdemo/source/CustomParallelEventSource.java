package com.hailiang.bigdata.flinkdemo.source;

import com.hailiang.bigdata.flinkdemo.pojo.Event;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Calendar;
import java.util.Random;

public class CustomParallelEventSource implements ParallelSourceFunction<Event> {
    // 声明一个标志位
    private Boolean running = true;

    public void run(SourceContext<Event> sourceContext) throws Exception {
        // 随机生成数据
        Random random = new Random();

        //定义字段随机选取值的数据集
        String[] users = {"张三", "李四", "王五", "赵六", "徐大壮"};
        String[] urls = {"www.baidu.com", "www.google.com", "www.apple.com", "www.hao123.com", "blog.ddup.cloud"};

        // 循环生成数据
        while (running) {
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            long timeInMillis = Calendar.getInstance().getTimeInMillis();
            sourceContext.collect(new Event(user,url,timeInMillis));
            Thread.sleep(500L);
        }
    }

    public void cancel() {
        running = false;
    }
}
