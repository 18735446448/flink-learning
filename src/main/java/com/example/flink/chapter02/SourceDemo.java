package com.example.flink.chapter02;

import com.example.flink.common.model.Event;
import com.example.flink.common.source.ClickSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * 第 2 章：数据源（Source）的几种常见定义方式。
 *
 * <p>知识点：
 * <ul>
 *   <li>从集合读取：{@code fromElements / fromCollection}，适合本地测试，属于有界流</li>
 *   <li>自定义 Source：{@code addSource(new ClickSource())}，可实现任意外部系统的接入</li>
 *   <li>并行度：自定义 Source 实现 {@code ParallelSourceFunction} 才能设置并行度 &gt; 1</li>
 *   <li>生产环境常用：Kafka / 文件系统 / CDC 连接器（见 README 扩展章节）</li>
 * </ul>
 */
public class SourceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 方式一：从元素/集合读取（有界流，执行完会自动停止）
        List<Event> events = Arrays.asList(
                new Event("Alice", "/home", 1000L),
                new Event("Bob", "/cart", 2000L),
                new Event("Alice", "/pay", 3000L)
        );
        DataStream<Event> collectionStream = env.fromCollection(events);
        collectionStream.print("from-collection");

        // 方式二：从文件读取（取消注释并填入真实路径即可体验）
        // DataStream<String> fileStream = env.readTextFile("input/words.txt");

        // 方式三：自定义 Source（无界流，需要手动停止）
        DataStream<Event> customStream = env.addSource(new ClickSource(1000L))
                .setParallelism(2); // ClickSource 是并行 Source，可以设置并行度
        customStream.print("from-custom-source");

        env.execute("Source Demo");
    }
}
