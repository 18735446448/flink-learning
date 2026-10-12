package com.example.flink.chapter05.state;

import com.example.flink.common.model.Event;
import com.example.flink.common.source.ClickSource;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;

import java.util.concurrent.TimeUnit;

/**
 * 第 5 章：检查点（Checkpoint）与容错机制。
 *
 * <p>Checkpoint 是 Flink 实现「精确一次（Exactly Once）」语义的核心：
 * 它会周期性地把所有算子的状态快照写入持久化存储；作业失败后从最近一次成功的 Checkpoint 恢复，
 * 状态与偏移量一起回退，从而保证计算不丢不重。
 *
 * <p>常用配置说明见下方代码注释。恢复命令示例：
 * <pre>
 *   flink run -s file:///tmp/flink-learning/checkpoints/chk-1 flink-learning-1.0-SNAPSHOT.jar
 * </pre>
 */
public class CheckpointDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ---------- 1. 开启 Checkpoint，周期 1 秒 ----------
        env.enableCheckpointing(1000);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 语义：EXACTLY_ONCE（默认）或 AT_LEAST_ONCE
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 两次 Checkpoint 之间的最小间隔，避免频繁触发影响性能
        checkpointConfig.setMinPauseBetweenCheckpoints(500);
        // 超时时间：超过该时间未完成的 Checkpoint 会被丢弃
        checkpointConfig.setCheckpointTimeout(60_000);
        // 同时进行的 Checkpoint 数量
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        // 作业取消时保留 Checkpoint 文件，便于手动恢复（默认是作业取消即删除）
        checkpointConfig.setExternalizedCheckpointCleanup(
                ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // Checkpoint 存储位置：生产请使用 HDFS / S3 等分布式存储
        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/flink-learning/checkpoints");

        // ---------- 2. 状态后端：决定状态存哪里 ----------
        // HashMapStateBackend：状态存放在 JVM 堆内存中（默认，速度快，受内存限制）
        env.setStateBackend(new HashMapStateBackend());
        // EmbeddedRocksDBStateBackend：状态存放在 RocksDB（本地磁盘 + 内存），支持超大状态与增量 Checkpoint
        // 需要额外依赖 flink-statebackend-rocksdb，取消下一行注释前请先添加依赖
        // env.setStateBackend(new EmbeddedRocksDBStateBackend(true));

        // ---------- 3. 重启策略：失败后自动恢复 ----------
        // 固定延迟重启：最多重启 3 次，每次间隔 10 秒
        env.setRestartStrategy(org.apache.flink.api.common.restartstrategy.RestartStrategies
                .fixedDelayRestart(3, org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)));

        // ---------- 4. 有状态的计算逻辑 ----------
        env.addSource(new ClickSource(800L))
                .map(e -> Tuple2.of(e.getUrl(), 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(t -> t.f0)
                // sum 内部维护状态，会被 Checkpoint 定期保存
                .sum(1)
                .print("checkpointed-pv");

        env.execute("Checkpoint Demo");
    }
}
