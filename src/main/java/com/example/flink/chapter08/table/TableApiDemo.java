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

import static org.apache.flink.table.api.Expressions.$;

/**
 * 第 8 章：Table API —— 用类似 SQL 的关系代数方式写流计算。
 *
 * <p>Table API / SQL 与 DataStream API 的关系：
 * <ul>
 *   <li>DataStream：更底层、控制力强（状态、定时器、CEP）</li>
 *   <li>Table / SQL：声明式、开发效率高，适合绝大多数聚合/关联/报表类场景</li>
 *   <li>两者可以互相转换：{@code fromDataStream} / {@code toDataStream} / {@code toChangelogStream}</li>
 * </ul>
 *
 * <p>重要概念：流上的聚合结果是一条 <b>changelog（变更日志）</b>流，
 * 包含 +I（插入）、-U（更新前）、+U（更新后）三种消息，所以用 toChangelogStream 接收。
 */
public class TableApiDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<Event> stream = env
                .addSource(new ClickSource(800L))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner((event, ts) -> event.getTs()));

        // 1. DataStream 转 Table：显式声明字段与水位线
        Table clicks = tableEnv.fromDataStream(
                stream,
                Schema.newBuilder()
                        .column("user", DataTypes.STRING())
                        .column("url", DataTypes.STRING())
                        .column("ts", DataTypes.BIGINT())
                        // 由 BIGINT 时间戳派生出时间属性列，供窗口使用
                        .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(ts, 3)")
                        .watermark("rowtime", "rowtime - INTERVAL '2' SECOND")
                        .build());

        // 2. Table API 查询：统计每个用户的访问次数
        Table userPv = clicks
                .groupBy($("user"))
                .select($("user"), $("url").count().as("cnt"));

        // 3. Table 转回 DataStream（聚合结果是 changelog 流）
        tableEnv.toChangelogStream(userPv).print("table-api-user-pv");

        // 4. 再来一个：过滤 + 按 url 统计
        Table urlPv = clicks
                .filter($("url").isNotEqual("/home"))
                .groupBy($("url"))
                .select($("url"), $("user").count().as("uv"));

        tableEnv.toChangelogStream(urlPv).print("table-api-url-pv");

        env.execute("Table API Demo");
    }
}
