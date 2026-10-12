package com.example.flink.chapter06.multistream;

import com.example.flink.common.model.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 第 6 章：多流转换 —— 把多条流合并或关联起来计算。
 *
 * <p>四种常见方式：
 * <ol>
 *   <li><b>union</b>：多条「数据类型相同」的流直接合并，不做去重</li>
 *   <li><b>connect</b>：两条「类型可以不同」的流连接，之后用 CoFlatMap/CoProcess 分别处理，
 *       常用于「数据流 + 规则/配置流」的动态更新场景</li>
 *   <li><b>Window Join</b>：在同一个时间窗口内按 key 关联两条流（类似批处理的 join）</li>
 *   <li><b>Interval Join</b>：按 key + 时间区间关联，无须窗口，语义更灵活（如「下单后 1 小时内的支付」）</li>
 * </ol>
 */
public class MultiStreamDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 用户点击流（user, url, ts）
        SingleOutputStreamOperator<Event> clickStream = env.fromElements(
                new Event("Alice", "/prod?id=1", 1000L),
                new Event("Bob", "/prod?id=2", 2000L),
                new Event("Alice", "/cart", 3000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner((e, ts) -> e.getTs()));

        // 用户浏览流（user, url, ts）
        SingleOutputStreamOperator<Event> browseStream = env.fromElements(
                new Event("Alice", "/home", 1500L),
                new Event("Bob", "/prod?id=2", 2500L),
                new Event("Cary", "/home", 3500L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner((e, ts) -> e.getTs()));

        // 一条与之类型不同的「配置流」：(用户名, 用户等级)
        DataStream<Tuple2<String, Integer>> levelStream = env.fromElements(
                Tuple2.of("Alice", 5),
                Tuple2.of("Bob", 3)
        );

        // ---------- 1. union：类型相同的流合并 ----------
        clickStream.union(browseStream)
                .map(e -> "union >> " + e)
                .print("union");

        // ---------- 2. connect：类型可以不同的流连接 ----------
        clickStream.keyBy(Event::getUser)
                .connect(levelStream.keyBy(t -> t.f0))
                .process(new EnrichWithLevelFunction())
                .print("connect");

        // ---------- 3. Window Join：同一窗口内按 url 关联 ----------
        clickStream.join(browseStream)
                .where(Event::getUrl)
                .equalTo(Event::getUrl)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new JoinFunction<Event, Event, String>() {
                    @Override
                    public String join(Event first, Event second) {
                        return String.format("window-join >> url=%s, click=%s, browse=%s",
                                first.getUrl(), first.getUser(), second.getUser());
                    }
                })
                .print("window-join");

        // ---------- 4. Interval Join：按 key 在「时间区间」内关联 ----------
        clickStream.keyBy(Event::getUser)
                .intervalJoin(browseStream.keyBy(Event::getUser))
                // 关联条件：browse 的时间落在 click 时间的前后 2 秒内
                .between(Time.seconds(-2), Time.seconds(2))
                .process(new ProcessJoinFunction<Event, Event, String>() {
                    @Override
                    public void processElement(Event left, Event right, Context ctx, Collector<String> out) {
                        out.collect(String.format("interval-join >> user=%s, click=%s, browse=%s",
                                left.getUser(), left.getUrl(), right.getUrl()));
                    }
                })
                .print("interval-join");

        env.execute("Multi Stream Demo");
    }

    /**
     * CoProcessFunction：把「点击流」与「用户等级配置流」关联起来。
     *
     * <p>注意：这里用 ValueState 保存每个 key 的等级，而不是普通成员变量 ——
     * 普通成员变量在并行度 &gt; 1 或 key 交错时不可靠，且无法被 Checkpoint 保存。
     */
    static class EnrichWithLevelFunction
            extends CoProcessFunction<Event, Tuple2<String, Integer>, String> {

        private transient ValueState<Integer> levelState;

        @Override
        public void open(Configuration parameters) {
            levelState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("user-level", Types.INT));
        }

        @Override
        public void processElement1(Event event, Context ctx, Collector<String> out) throws Exception {
            Integer level = levelState.value();
            out.collect(String.format("connect >> user=%s level=%s 访问了 %s",
                    event.getUser(), level == null ? "未知" : level, event.getUrl()));
        }

        @Override
        public void processElement2(Tuple2<String, Integer> value, Context ctx, Collector<String> out)
                throws Exception {
            // 收到配置流数据：更新该 key 的状态（配置变更会自动生效）
            levelState.update(value.f1);
            out.collect("connect >> 更新配置：" + value);
        }
    }
}
