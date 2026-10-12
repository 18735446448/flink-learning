package com.example.flink.chapter04.time;

import com.example.flink.common.model.Event;
import com.example.flink.common.source.BoundedOutOfOrderSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 第 4 章：时间语义与水位线（Watermark）。
 *
 * <p>三种时间语义：
 * <ul>
 *   <li><b>EventTime（事件时间）</b>：数据自带的时间戳，结果准确可重现，生产首选</li>
 *   <li><b>ProcessingTime（处理时间）</b>：算子所在机器的系统时间，延迟最低但结果不可重现</li>
 *   <li><b>IngestionTime（摄入时间）</b>：数据进入 Flink 的时间，介于两者之间（实际很少用）</li>
 * </ul>
 *
 * <p>水位线是什么：
 * <blockquote>
 *   水位线是一个插入到数据流中的「时间戳标记」，含义是「今后不会再有时间戳 &lt;= W 的数据」，
 *   它是 EventTime 窗口触发的唯一依据。水位线 = 当前最大事件时间 - 最大乱序容忍时间 - 1ms。
 * </blockquote>
 *
 * <p>本例使用 {@link BoundedOutOfOrderSource}，事件时间依次为 1s、2s、12s、8s、13s、9s，
 * 乱序容忍 2 秒，窗口长度为 10 秒。预期可以看到：
 * <ol>
 *   <li>12s 数据到达后，水位线推进到 10s，触发 [0,10) 窗口（此时窗口内有 1s、2s 两条）</li>
 *   <li>随后到达的 8s、9s 数据属于已触发的窗口 —— 默认会被直接丢弃（见 AllowedLatenessDemo 的补救方式）</li>
 * </ol>
 */
public class WatermarkDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 每 200ms 生成一次水位线（默认 200ms，调大可以减少水位线数量）
        env.getConfig().setAutoWatermarkInterval(200L);

        // 1. 定义水位线策略：允许 2 秒的乱序
        WatermarkStrategy<Event> watermarkStrategy = WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((event, timestamp) -> event.getTs());

        SingleOutputStreamOperator<Event> stream = env
                .addSource(new BoundedOutOfOrderSource(1000L))
                .assignTimestampsAndWatermarks(watermarkStrategy);

        // 2. 用 ProcessFunction 观察每条数据到达时的「当前水位线」
        stream.process(new ProcessFunction<Event, String>() {
            @Override
            public void processElement(Event event, Context ctx, Collector<String> out) {
                long watermark = ctx.timerService().currentWatermark();
                out.collect(String.format("event=%s | currentWatermark=%s", event, watermark));
            }
        }).print("watermark-trace");

        // 3. 基于事件时间的滚动窗口：窗口 [0,10) 会在水位线 &gt;= 10s 时触发
        stream.keyBy(Event::getUser)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction<
                        Event, String, String, TimeWindow>() {
                    @Override
                    public void process(String user,
                                        Context context,
                                        Iterable<Event> events,
                                        Collector<String> out) {
                        TimeWindow window = context.window();
                        long count = 0;
                        StringBuilder urls = new StringBuilder();
                        for (Event e : events) {
                            count++;
                            urls.append(e.getUrl()).append(" ");
                        }
                        out.collect(String.format(
                                "窗口 [%d, %d) 触发：user=%s, count=%d, urls=[%s]",
                                window.getStart(), window.getEnd(), user, count, urls.toString().trim()));
                    }
                })
                .print("event-time-window");

        env.execute("Watermark Demo");
    }
}
