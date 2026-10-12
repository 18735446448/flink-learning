package com.example.flink.common.source;

import com.example.flink.common.model.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 有界的「乱序事件」数据源：按固定顺序发送带时间戳的事件，用于第 4 章演示时间语义与水位线。
 *
 * <p>数据特点：时间戳整体递增，但中途会出现「迟到数据」（时间戳小于已生成的水位线），
 * 便于观察水位线推进、窗口触发时机以及迟到数据的处理策略。
 *
 * <p>发送的事件（单位：毫秒）：
 * <pre>
 *   ts=1000  ts=2000  ts=12000  ts=8000（迟到）  ts=13000  ts=9000（迟到）
 * </pre>
 */
public class BoundedOutOfOrderSource implements SourceFunction<Event> {

    private volatile boolean isRunning = true;

    /** 每条数据之间的发送间隔，留出间隔才能观察到水位线推进的过程 */
    private final long intervalMs;

    public BoundedOutOfOrderSource() {
        this(1000L);
    }

    public BoundedOutOfOrderSource(long intervalMs) {
        this.intervalMs = intervalMs;
    }

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        Event[] events = {
                new Event("Alice", "/home", 1000L),
                new Event("Alice", "/cart", 2000L),
                // 时间戳跳到 12s：水位线将推进到 10s（乱序容忍 2s），触发 [0,10) 窗口
                new Event("Alice", "/pay", 12000L),
                // 迟到数据：本应属于 [0,10) 窗口，但该窗口已经触发过
                new Event("Alice", "/prod?id=9", 8000L),
                new Event("Alice", "/logout", 13000L),
                // 再次迟到
                new Event("Alice", "/prod?id=8", 9000L)
        };

        for (Event event : events) {
            if (!isRunning) {
                break;
            }
            // 发送带事件时间的数据（水位线交给下游的 WatermarkStrategy 生成）
            ctx.collectWithTimestamp(event, event.getTs());
            Thread.sleep(intervalMs);
        }

        // 有界数据发送完毕：Source 结束，Flink 会把水位线推到 Long.MAX_VALUE 触发所有剩余窗口
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
