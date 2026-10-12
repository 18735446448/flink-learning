package com.example.flink.chapter01;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 第 1 章：Flink 版 WordCount（批处理模式）——所有大数据框架的 "Hello World"。
 *
 * <p>运行方式：
 * <pre>
 *   mvn exec:java -Dexec.mainClass=com.example.flink.chapter01.BatchWordCount
 * </pre>
 *
 * <p>核心知识点：
 * <ul>
 *   <li>Flink 1.12 之后批流统一：DataStream API 通过 {@code setRuntimeMode(BATCH)} 即可处理有界数据</li>
 *   <li>批模式下结果会按 key 全局聚合后一次性输出，而流模式会持续输出每一次更新</li>
 *   <li>典型流程：source -&gt; flatMap -&gt; map -&gt; keyBy -&gt; sum -&gt; sink</li>
 * </ul>
 */
public class BatchWordCount {

    private static final String[] WORDS = {
            "hello flink",
            "hello world",
            "flink is fast",
            "flink is scalable",
            "hello flink again"
    };

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置为批处理模式（有界流）
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);

        // 2. 读取数据源：这里用内存集合，实际可替换为 readTextFile / Kafka Source 等
        DataStream<String> lineStream = env.fromElements(WORDS);

        // 3. 转换计算
        DataStream<Tuple2<String, Integer>> wordAndOne = lineStream
                // flatMap：一行拆成多个单词（一对多）
                .flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
                    for (String word : line.split("\\s+")) {
                        out.collect(Tuple2.of(word, 1));
                    }
                })
                // 使用 returns 明确类型，避免 Lambda 泛型擦除导致 Flink 无法推断类型
                .returns(org.apache.flink.api.common.typeinfo.Types.TUPLE(
                        org.apache.flink.api.common.typeinfo.Types.STRING,
                        org.apache.flink.api.common.typeinfo.Types.INT));

        DataStream<Tuple2<String, Integer>> result = wordAndOne
                // keyBy：按单词分组（逻辑上把流按 key 分成多条子流）
                .keyBy(value -> value.f0)
                // sum：对下标为 1 的字段求和（滚动聚合，带状态）
                .sum(1);

        // 4. 输出
        result.print("word-count");

        // 5. 执行（DataStream API 必须显式调用 execute 触发）
        env.execute("Batch WordCount");
    }
}
