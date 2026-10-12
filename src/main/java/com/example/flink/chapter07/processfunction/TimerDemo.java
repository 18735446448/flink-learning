package com.example.flink.chapter07.processfunction;

import com.example.flink.common.model.Event;
import com.example.flink.common.source.ClickSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 第 7 章：定时器（Timer）—— ProcessFunction 的核心能力，让你不写窗口也能做时间驱动的计算。
 *
 * <p>两种定时器：
 * <ul>
 *   <li><b>EventTime Timer</b>：水位线 &gt;= 定时时间时触发（本例使用）</li>
 *   <li><b>ProcessingTime Timer</b>：系统时间到达定时时间时触发</li>
 * </ul>
 *
 * <p>本例逻辑：对每个用户，从第一条数据起「事件时间 5 秒」后触发定时器，
 * 输出该用户在窗口期内的访问次数，然后清空状态。这是自定义窗口/超时检测的通用写法。
 */
public class TimerDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ClickSource(800L))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner((event, ts) -> event.getTs()))
                .keyBy(Event::getUser)
                .process(new VisitCountWithTimerFunction())
                .print("timer-result");

        env.execute("Timer Demo");
    }

    /** 使用状态 + 定时器实现「5 秒（事件时间）内的访问次数统计」 */
    static class VisitCountWithTimerFunction extends KeyedProcessFunction<String, Event, String> {

        private static final long WINDOW_MS = 5000L;

        private transient ValueState<Long> countState;
        private transient ValueState<Long> timerState;

        @Override
        public void open(Configuration parameters) {
            countState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("count", Types.LONG));
            timerState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("timer", Types.LONG));
        }

        @Override
        public void processElement(Event event, Context ctx, Collector<String> out) throws Exception {
            // 1. 累加当前计数
            Long count = countState.value();
            countState.update((count == null ? 0L : count) + 1);

            // 2. 第一次收到该 key 的数据时注册定时器（避免重复注册）
            if (timerState.value() == null) {
                long triggerTime = ctx.timestamp() + WINDOW_MS;
                ctx.timerService().registerEventTimeTimer(triggerTime);
                timerState.update(triggerTime);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发：输出结果并清理状态，防止状态无限增长
            out.collect(String.format("user=%s 在 5 秒内共访问 %d 次",
                    ctx.getCurrentKey(), countState.value()));
            countState.clear();
            timerState.clear();
        }
    }
}
