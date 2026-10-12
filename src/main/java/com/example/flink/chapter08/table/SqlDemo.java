package com.example.flink.chapter08.table;

import com.example.flink.common.model.Event;
import com.example.flink.common.source.ClickSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * 第 8 章：Flink SQL —— 用标准 SQL 写流计算，是最推荐的生产开发方式。
 *
 * <p>本例演示：
 * <ol>
 *   <li>把 DataStream 注册为临时视图，然后用 SQL 查询</li>
 *   <li>普通 GROUP BY 聚合（持续更新的 changelog 结果）</li>
 *   <li>窗口 TVF（Flink 1.13 之后推荐的窗口写法）：TUMBLE / HOP / CUMULATE</li>
 * </ol>
 *
 * <p>知识点：时间属性列（本例的 rowtime）是窗口运行的前提，
 * 必须通过 {@code WATERMARK FOR rowtime AS ...} 声明。
 */
public class SqlDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<Event> stream = env
                .addSource(new ClickSource(800L))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner((event, ts) -> event.getTs()));

        // 注册为临时视图（只在当前会话可见）
        tableEnv.createTemporaryView(
                "clicks",
                stream,
                Schema.newBuilder()
                        .column("user", DataTypes.STRING())
                        .column("url", DataTypes.STRING())
                        .column("ts", DataTypes.BIGINT())
                        .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(ts, 3)")
                        .watermark("rowtime", "rowtime - INTERVAL '2' SECOND")
                        .build());

        // ---------- 1. 基本聚合：每个 url 的访问量 ----------
        Table urlPv = tableEnv.sqlQuery(
                "SELECT url, COUNT(*) AS pv " +
                        "FROM clicks " +
                        "GROUP BY url");
        tableEnv.toChangelogStream(urlPv).print("sql-url-pv");

        // ---------- 2. 滚动窗口 TVF：每 10 秒统计一次各 url 的访问量 ----------
        Table windowPv = tableEnv.sqlQuery(
                "SELECT window_start, window_end, url, COUNT(*) AS pv " +
                        "FROM TABLE( " +
                        "  TUMBLE(TABLE clicks, DESCRIPTOR(rowtime), INTERVAL '10' SECONDS) " +
                        ") " +
                        "GROUP BY window_start, window_end, url");
        tableEnv.toChangelogStream(windowPv).print("sql-tumble-window");

        // ---------- 3. TopN：每个窗口内访问量最高的 2 个 url ----------
        Table topN = tableEnv.sqlQuery(
                "SELECT window_start, url, pv, rank_num FROM ( " +
                        "  SELECT window_start, url, pv, " +
                        "         ROW_NUMBER() OVER (PARTITION BY window_start ORDER BY pv DESC) AS rank_num " +
                        "  FROM ( " +
                        "    SELECT window_start, url, COUNT(*) AS pv " +
                        "    FROM TABLE(TUMBLE(TABLE clicks, DESCRIPTOR(rowtime), INTERVAL '10' SECONDS)) " +
                        "    GROUP BY window_start, window_end, url " +
                        "  ) " +
                        ") WHERE rank_num <= 2");
        tableEnv.toChangelogStream(topN).print("sql-topn");

        env.execute("Flink SQL Demo");
    }
}
