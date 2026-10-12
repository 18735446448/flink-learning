package com.example.flink.chapter03.window;

import com.example.flink.common.source.ClickSource;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 第 3 章：窗口（Window）的类型。
 *
 * <p>窗口是「把无界流切成有界块」再计算的机制，是流计算的核心概念。分类如下：
 * <pre>
 *  按驱动方式：
 *    - 时间窗口 TimeWindow  ：按时间划分（本例使用处理时间，第 4 章讲事件时间）
 *    - 计数窗口 CountWindow：按数据条数划分
 *  按分配规则：
 *    - 滚动窗口 Tumbling：窗口之间不重叠，一条数据只属于一个窗口
 *    - 滑动窗口 Sliding ：窗口可重叠，一条数据可能属于多个窗口（由 窗口长度 + 滑动步长 决定）
 *    - 会话窗口 Session ：以「活跃间隙 gap」切分，超过 gap 没有数据则窗口关闭
 *    - 全局窗口 Global  ：所有数据一个窗口，必须自定义 Trigger 才有意义
 * </pre>
 *
 * <p>运行方式（无界流，观察一段时间后用 Ctrl + C 停止）：
 * <pre>
 *   mvn exec:java -Dexec.mainClass=com.example.flink.chapter03.window.WindowTypeDemo
 * </pre>
 */
public class WindowTypeDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 使用处理时间窗口时不需要水位线，因此这里没有 assignTimestampsAndWatermarks
        KeyedStream<Tuple2<String, Integer>, String> keyed = env
                .addSource(new ClickSource(600L))
                .map(e -> Tuple2.of(e.getUrl(), 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(t -> t.f0);

        // 1. 滚动窗口：每 5 秒统计一次，窗口之间不重叠
        keyed.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1)
                .print("tumbling-5s");

        // 2. 滑动窗口：窗口长度 10 秒，每 5 秒滑动一次（同一条数据会出现在 2 个窗口中）
        keyed.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .sum(1)
                .print("sliding-10s-5s");

        // 3. 会话窗口：某个 key 超过 3 秒没有新数据，窗口即关闭
        keyed.window(ProcessingTimeSessionWindows.withGap(Time.seconds(3)))
                .sum(1)
                .print("session-gap-3s");

        // 4. 计数窗口：每累计 5 条数据触发一次（与时间无关）
        keyed.countWindow(5)
                .sum(1)
                .print("count-window-5");

        env.execute("Window Type Demo");
    }
}
