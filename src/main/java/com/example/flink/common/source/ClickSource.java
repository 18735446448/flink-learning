package com.example.flink.common.source;

import com.example.flink.common.model.Event;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 自定义数据源：模拟不断产生的用户点击事件。
 *
 * <p>知识点：
 * <ul>
 *   <li>{@code RichParallelSourceFunction}：可并行执行的富函数 Source，拥有 open/close 生命周期方法
 *       （普通 {@code SourceFunction} 并行度只能为 1）</li>
 *   <li>{@code isRunning} 使用 volatile/Atomic 保证被 cancel() 中断时能及时退出循环</li>
 *   <li>本 Source 生成的数据时间戳是「乱序」的（最多回退 5 秒），专门用于后续水位线/乱序数据实验</li>
 * </ul>
 *
 * <p>说明：Flink 1.17 之后官方推荐新 Source API（org.apache.flink.api.connector.source.Source），
 * 但旧版 SourceFunction 更简单直观，适合入门学习。
 */
public class ClickSource extends RichParallelSourceFunction<Event> {

    private final AtomicBoolean isRunning = new AtomicBoolean(true);

    /** 每个并行子任务的数据发送间隔（毫秒） */
    private final long intervalMs;

    public ClickSource() {
        this(1000L);
    }

    public ClickSource(long intervalMs) {
        this.intervalMs = intervalMs;
    }

    private static final String[] USERS = {"Alice", "Bob", "Cary", "David", "Ella"};
    private static final String[] URLS = {"/home", "/cart", "/prod?id=1", "/prod?id=2", "/pay", "/logout"};

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        Random random = new Random();
        while (isRunning.get()) {
            String user = USERS[random.nextInt(USERS.length)];
            String url = URLS[random.nextInt(URLS.length)];
            // 故意让时间戳乱序：当前时间随机回退 0~5 秒
            long timestamp = System.currentTimeMillis() - random.nextInt(5000);
            Event event = new Event(user, url, timestamp);

            // 使用 collectWithTimestamp 发送带时间戳的数据（配合水位线策略使用）
            ctx.collectWithTimestamp(event, timestamp);

            Thread.sleep(intervalMs);
        }
    }

    @Override
    public void cancel() {
        isRunning.set(false);
    }
}
