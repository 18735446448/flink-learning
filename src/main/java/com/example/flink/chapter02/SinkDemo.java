package com.example.flink.chapter02;

import com.example.flink.common.model.Event;
import com.example.flink.common.source.ClickSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.BufferedWriter;
import java.io.FileWriter;

/**
 * 第 2 章：数据输出（Sink）。
 *
 * <p>知识点：
 * <ul>
 *   <li>{@code print()}：最简调试输出，会在输出前带上子任务编号（如 1&gt; 或 2&gt;）</li>
 *   <li>{@code RichSinkFunction}：自定义 Sink，拥有 open/close 生命周期，适合管理连接（JDBC/Redis/HTTP 等）</li>
 *   <li>自定义 Sink 默认「至少一次」语义；要实现精确一次需配合 {@code CheckpointedFunction}
 *       或使用支持两阶段提交的官方连接器（如 Kafka、FileSink）</li>
 * </ul>
 */
public class SinkDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Event> stream = env.addSource(new ClickSource(500L));

        // 内置 Sink：打印到控制台
        stream.print("console");

        // 自定义 Sink：写入本地文件（演示 open/close 生命周期的用法）
        stream.addSink(new FileEventSink("target/output/events.txt"));

        env.execute("Sink Demo");
    }

    /**
     * 自定义 Sink：把事件追加写入文件。
     * 真实项目中这里通常是 JDBC / Redis / Kafka / HBase 等外部系统的写入客户端。
     */
    static class FileEventSink extends RichSinkFunction<Event> {

        private final String path;
        private transient BufferedWriter writer;

        FileEventSink(String path) {
            this.path = path;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // open 在每个并行子任务启动时调用一次，用于建立连接/打开文件
            writer = new BufferedWriter(new FileWriter(path, true));
        }

        @Override
        public void invoke(Event value, Context context) throws Exception {
            writer.write(value.toString());
            writer.newLine();
            writer.flush();
        }

        @Override
        public void close() throws Exception {
            // close 在作业正常结束或被取消时调用，用于释放资源
            if (writer != null) {
                writer.close();
            }
        }
    }
}
