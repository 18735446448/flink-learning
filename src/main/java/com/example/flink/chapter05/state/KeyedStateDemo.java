package com.example.flink.chapter05.state;

import com.example.flink.common.model.Event;
import com.example.flink.common.source.ClickSource;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * 第 5 章：状态（State）—— Flink 能做到「有状态的流计算」的根本。
 *
 * <p>状态分为两类：
 * <ul>
 *   <li><b>Keyed State（键控状态）</b>：只能在 keyBy 之后使用，按 key 隔离，是最常用的状态</li>
 *   <li><b>Operator State（算子状态）</b>：与算子并行实例绑定，与 key 无关（如 Source 的偏移量）</li>
 * </ul>
 *
 * <p>Keyed State 支持的五种结构：
 * <pre>
 *   ValueState&lt;T&gt;        ：单个值（保存/更新/读取）
 *   ListState&lt;T&gt;         ：列表
 *   MapState&lt;K, V&gt;       ：键值对
 *   ReducingState&lt;T&gt;     ：聚合结果，输入输出类型相同
 *   AggregatingState&lt;IN, OUT&gt;：聚合结果，输入输出类型可以不同
 * </pre>
 *
 * <p>要点：状态必须先在 open() 中通过 {@code getRuntimeContext().getXxxState(descriptor)} 注册，
 * 且只有在 keyBy 之后才能使用；否则会抛出 "state is only available on keyed streams" 异常。
 */
public class KeyedStateDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KeyedStream<Event, String> keyedStream = env
                .addSource(new ClickSource(800L))
                .keyBy(Event::getUser);

        // 1. ValueState：统计每个用户的访问次数
        keyedStream.flatMap(new UserPvCountFunction()).print("value-state");

        // 2. ListState：保留每个用户最近 3 次访问记录
        keyedStream.flatMap(new RecentVisitsFunction()).print("list-state");

        // 3. MapState：统计每个用户访问各个 url 的次数
        keyedStream.flatMap(new UrlCountFunction()).print("map-state");

        env.execute("Keyed State Demo");
    }

    /** ValueState 示例：每个用户一个计数器，持续累加 */
    static class UserPvCountFunction extends RichFlatMapFunction<Event, String> {

        private transient ValueState<Long> countState;

        @Override
        public void open(Configuration parameters) {
            countState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("pv-count", Types.LONG));
        }

        @Override
        public void flatMap(Event event, Collector<String> out) throws Exception {
            Long count = countState.value();
            count = (count == null ? 0L : count) + 1;
            countState.update(count);
            out.collect(String.format("user=%s 第 %d 次访问 %s", event.getUser(), count, event.getUrl()));
        }
    }

    /** ListState 示例：只保留最近 3 条访问记录 */
    static class RecentVisitsFunction extends RichFlatMapFunction<Event, String> {

        private static final int MAX_KEEP = 3;
        private transient ListState<Event> visitState;

        @Override
        public void open(Configuration parameters) {
            visitState = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("recent-visits", Event.class));
        }

        @Override
        public void flatMap(Event event, Collector<String> out) throws Exception {
            visitState.add(event);
            List<Event> all = new ArrayList<>();
            visitState.get().forEach(all::add);
            while (all.size() > MAX_KEEP) {
                all.remove(0);
            }
            visitState.update(all);

            StringBuilder sb = new StringBuilder();
            for (Event e : all) {
                sb.append(e.getUrl()).append(" <- ");
            }
            out.collect(String.format("user=%s 最近访问：%s", event.getUser(), sb));
        }
    }

    /** MapState 示例：按 url 维度分别计数 */
    static class UrlCountFunction extends RichFlatMapFunction<Event, String> {

        private transient MapState<String, Long> urlCountState;

        @Override
        public void open(Configuration parameters) {
            urlCountState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("url-count", Types.STRING, Types.LONG));
        }

        @Override
        public void flatMap(Event event, Collector<String> out) throws Exception {
            String url = event.getUrl();
            Long count = urlCountState.get(url);
            count = (count == null ? 0L : count) + 1;
            urlCountState.put(url, count);
            out.collect(String.format("user=%s, url=%s, count=%d", event.getUser(), url, count));
        }
    }
}
