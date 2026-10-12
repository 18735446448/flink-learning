package com.example.flink.chapter04.time;

import com.example.flink.common.model.Event;
import com.example.flink.common.source.BoundedOutOfOrderSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 第 4 章：迟到数据的三种处理策略。
 *
 * <p>默认情况：窗口触发后状态立即清空，之后到达的迟到数据会被丢弃。Flink 提供了两个补救手段：
 * <ol>
 *   <li><b>allowedLateness</b>：窗口触发后仍保留状态一段时间，期间迟到数据会再次触发窗口输出（增量更新）</li>
 *   <li><b>sideOutputLateData</b>：超过 allowedLateness 的迟到数据写入侧输出流，可单独收集处理</li>
 *   <li>两者通常搭配使用：先用 allowedLateness 修正结果，再兜底收集极端迟到数据</li>
 * </ol>
 *
 * <p>本例预期输出：
 * <ul>
 *   <li>[0,10) 窗口先输出 2 条（1s、2s 的数据）</li>
 *   <li>迟到数据 8s、9s 到达后，窗口再次触发，依次输出 3 条、4 条（因为窗口状态仍在保留期内）</li>
 *   <li>如果注释掉 allowedLateness，这些数据会进入侧输出流 late-event</li>
 * </ul>
 */
public class AllowedLatenessDemo {

    /** 侧输出流标签：使用匿名内部类写法是为了在运行时保留泛型信息 */
    private static final OutputTag<Event> LATE_TAG = new OutputTag<Event>("late-event") {
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env
                .addSource(new BoundedOutOfOrderSource(1000L))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner((event, ts) -> event.getTs()));

        SingleOutputStreamOperator<String> result = stream
                .keyBy(Event::getUser)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // 窗口触发后，状态再保留 1 分钟，用于等待迟到数据
                .allowedLateness(Time.minutes(1))
                // 超过保留期的迟到数据放入侧输出流，避免被静默丢弃
                .sideOutputLateData(LATE_TAG)
                .process(new WindowCountFunction());

        result.print("window-result");

        // 获取侧输出流
        DataStream<Event> lateStream = result.getSideOutput(LATE_TAG);
        lateStream.print("late-event");

        env.execute("Allowed Lateness Demo");
    }

    /** 统计窗口内的事件条数 */
    static class WindowCountFunction
            extends ProcessWindowFunction<Event, String, String, TimeWindow> {

        @Override
        public void process(String user,
                            Context context,
                            Iterable<Event> events,
                            Collector<String> out) {
            long count = 0;
            for (Event ignored : events) {
                count++;
            }
            TimeWindow window = context.window();
            out.collect(String.format("窗口 [%d, %d) => user=%s, count=%d",
                    window.getStart(), window.getEnd(), user, count));
        }
    }
}
