package com.example.flink.chapter02;

import com.example.flink.common.model.Event;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 第 2 章：DataStream 转换算子（Transformation）一览。
 *
 * <p>算子分为三类：
 * <ol>
 *   <li><b>基本转换</b>：map / flatMap / filter —— 一对一或一对多的无状态转换</li>
 *   <li><b>聚合算子</b>：keyBy + sum/min/max/minBy/maxBy/reduce —— 必须作用在 KeyedStream 上，内部维护状态</li>
 *   <li><b>物理分区</b>：shuffle / rebalance / rescale / broadcast / global —— 决定数据如何在并行任务间分发，
 *       会触发数据的网络传输（重新分区）</li>
 * </ol>
 */
public class TransformDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Event> stream = env.fromElements(
                new Event("Alice", "/home", 1000L),
                new Event("Bob", "/cart", 2000L),
                new Event("Alice", "/prod?id=1", 3000L),
                new Event("Bob", "/pay", 4000L),
                new Event("Alice", "/pay", 5000L),
                new Event("Cary", "/home", 6000L)
        );

        // ---------- 1. 基本转换算子 ----------
        // filter：保留满足条件的数据
        SingleOutputStreamOperator<Event> filtered = stream.filter(e -> !"/home".equals(e.getUrl()));

        // map：一对一转换
        SingleOutputStreamOperator<Tuple2<String, String>> mapped =
                filtered.map(e -> Tuple2.of(e.getUser(), e.getUrl()))
                        .returns(Types.TUPLE(Types.STRING, Types.STRING));

        mapped.print("map");

        // ---------- 2. 聚合算子（需要 keyBy） ----------
        KeyedStream<Event, String> keyed = filtered.keyBy(Event::getUser);

        // 滚动聚合：sum 会对指定字段持续累加（sum 只能作用在 KeyedStream 上）
        stream.map(e -> Tuple2.of(e.getUser(), 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(t -> t.f0)
                .sum(1)
                .print("sum-by-user");

        // maxBy：返回指定字段最大的那条完整记录（max 只更新该字段，其余字段保留首次值）
        keyed.maxBy("ts").print("maxBy-ts");

        // reduce：自定义归约逻辑，可返回任意类型
        DataStream<Tuple2<String, Integer>> reduced = filtered
                .map(e -> Tuple2.of(e.getUser(), 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(t -> t.f0)
                .reduce((ReduceFunction<Tuple2<String, Integer>>) (v1, v2) ->
                        Tuple2.of(v1.f0, v1.f1 + v2.f1));
        reduced.print("reduce-count");

        // ---------- 3. 物理分区算子 ----------
        // rebalance：轮询分发，常用于解决数据倾斜
        filtered.rebalance().print("rebalance").setParallelism(4);
        // broadcast：把数据复制到所有下游并行任务（如广播配置流/规则流）
        // filtered.broadcast().print("broadcast");
        // shuffle：随机分发
        // filtered.shuffle().print("shuffle");

        env.execute("Transform Demo");
    }
}
