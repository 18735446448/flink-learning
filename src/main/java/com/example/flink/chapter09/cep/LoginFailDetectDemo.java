package com.example.flink.chapter09.cep;

import com.example.flink.common.model.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * 第 9 章：CEP（Complex Event Processing，复杂事件处理）—— 从事件流中识别「模式」。
 *
 * <p>典型场景：连续登录失败告警、下单后 30 分钟未支付、风控规则匹配、异常行为检测等。
 *
 * <p>核心概念：
 * <ul>
 *   <li><b>Pattern</b>：定义要匹配的事件序列（本例：同一用户 10 秒内连续 3 次登录失败）</li>
 *   <li><b>量词</b>：times / oneOrMore / timesOrMore / optional / greedy</li>
 *   <li><b>条件</b>：where（简单条件）、subtype、until 等</li>
 *   <li><b>近邻关系</b>：next（严格紧邻）、followedBy（非严格，中间可有其他事件）、followedByAny</li>
 *   <li><b>within</b>：整个模式必须在指定时间内完成</li>
 * </ul>
 *
 * <p>本例使用 Event 的 url 字段承载登录结果：/fail 表示失败、/success 表示成功。
 */
public class LoginFailDetectDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 登录事件流（有界数据，方便观察匹配结果）
        KeyedStream<Event, String> loginStream = env.fromElements(
                new Event("Alice", "/fail", 1000L),
                new Event("Alice", "/fail", 2000L),
                new Event("Alice", "/fail", 3000L),   // Alice 连续 3 次失败 -> 命中规则
                new Event("Bob", "/fail", 4000L),
                new Event("Bob", "/success", 5000L),  // Bob 未连续失败 -> 不命中
                new Event("Alice", "/success", 6000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner((event, ts) -> event.getTs())
        ).keyBy(Event::getUser);

        // 定义模式：连续 3 次失败（严格紧邻，且 10 秒内完成）
        Pattern<Event, Event> pattern = Pattern.<Event>begin("first")
                .where(new org.apache.flink.cep.pattern.conditions.SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) {
                        return "/fail".equals(event.getUrl());
                    }
                })
                .next("second")
                .where(new org.apache.flink.cep.pattern.conditions.SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) {
                        return "/fail".equals(event.getUrl());
                    }
                })
                .next("third")
                .where(new org.apache.flink.cep.pattern.conditions.SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) {
                        return "/fail".equals(event.getUrl());
                    }
                })
                .within(org.apache.flink.streaming.api.windowing.time.Time.seconds(10));

        // 在 keyBy 之后的流上应用模式
        PatternStream<Event> patternStream = CEP.pattern(loginStream, pattern);

        // 输出匹配到的事件序列
        patternStream.process(new PatternProcessFunction<Event, String>() {
            @Override
            public void processMatch(Map<String, List<Event>> match, Context ctx, Collector<String> out) {
                Event first = match.get("first").get(0);
                Event second = match.get("second").get(0);
                Event third = match.get("third").get(0);
                out.collect(String.format("风险告警：用户 %s 在 10 秒内连续 3 次登录失败（时间：%d, %d, %d）",
                        first.getUser(), first.getTs(), second.getTs(), third.getTs()));
            }
        }).print("cep-alert");

        env.execute("CEP Login Fail Detection");
    }
}
