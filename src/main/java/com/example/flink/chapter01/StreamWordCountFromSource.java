package com.example.flink.chapter01;

import com.example.flink.common.model.Event;
import com.example.flink.common.source.ClickSource;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 第 1 章：无需任何外部依赖的流式 WordCount —— 使用自定义 Source 生成点击事件并统计 URL 访问量（PV）。
 *
 * <p>运行方式：
 * <pre>
 *   mvn exec:java -Dexec.mainClass=com.example.flink.chapter01.StreamWordCountFromSource
 * </pre>
 *
 * <p>与 {@link StreamWordCount} 的区别：数据源换成自定义的 {@link ClickSource}，
 * 开箱即用，不需要手动启动 nc。
 */
public class StreamWordCountFromSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Event> eventStream = env.addSource(new ClickSource(1000L));

        DataStream<Tuple2<String, Integer>> pv = eventStream
                .map(event -> Tuple2.of(event.getUrl(), 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(t -> t.f0)
                .sum(1);

        pv.print("pv-by-url");

        env.execute("Streaming WordCount (custom source)");
    }
}
