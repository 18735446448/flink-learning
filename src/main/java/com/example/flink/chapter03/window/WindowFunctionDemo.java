package com.example.flink.chapter03.window;

import com.example.flink.common.model.Event;
import com.example.flink.common.source.ClickSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 第 3 章：窗口函数（Window Function）—— 决定窗口触发时如何计算。
 *
 * <p>两类窗口函数：
 * <ol>
 *   <li><b>增量聚合函数</b>（ReduceFunction / AggregateFunction）：来一条算一条，只保存中间结果，
 *       性能好；但拿不到窗口的元信息（开始/结束时间）</li>
 *   <li><b>全窗口函数</b>（ProcessWindowFunction）：缓存窗口内所有数据，触发时统一计算，
 *       能拿到窗口信息和上下文，但占用内存较多</li>
 * </ol>
 *
 * <p>最佳实践：两者结合 —— {@code aggregate(增量函数, 全窗口函数)}：
 * 先增量聚合，再把聚合结果交给全窗口函数包装窗口信息。
 */
public class WindowFunctionDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env
                .addSource(new ClickSource(600L))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>noWatermarks()
                                .withTimestampAssigner((event, ts) -> event.getTs()));

        SingleOutputStreamOperator<Tuple2<String, Long>> urlAndOne = stream
                .map(e -> Tuple2.of(e.getUrl(), 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 方式一：纯增量聚合（AggregateFunction），输出 (url, 计数)
        urlAndOne.keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new CountAgg())
                .print("aggregate-only");

        // 方式二：增量聚合 + 全窗口函数：既高效又能拿到窗口起止时间
        urlAndOne.keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new CountAgg(), new WindowResultWithInfo())
                .print("aggregate+process");

        env.execute("Window Function Demo");
    }

    /**
     * 增量聚合函数：统计每个 key 的数据条数。
     * 三个泛型分别是：输入类型 IN、累加器类型 ACC、输出类型 OUT。
     */
    static class CountAgg implements AggregateFunction<Tuple2<String, Long>, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<String, Long> value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    /**
     * 全窗口函数：把聚合结果包装成带窗口信息的字符串。
     * 泛型：IN（来自上一个聚合函数的结果）、OUT、KEY、窗口类型 W。
     */
    static class WindowResultWithInfo
            extends org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction<
            Long, String, String, TimeWindow> {

        @Override
        public void process(String key,
                            Context context,
                            Iterable<Long> elements,
                            Collector<String> out) {
            TimeWindow window = context.window();
            long count = elements.iterator().next();
            out.collect(String.format("url=%s, count=%d, window=[%d, %d)",
                    key, count, window.getStart(), window.getEnd()));
        }
    }
}
