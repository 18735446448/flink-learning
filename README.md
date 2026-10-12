# Flink 学习项目（flink-learning）

一个**循序渐进、开箱即用**的 Apache Flink 学习工程：每个示例都是可独立运行的 `main` 方法，代码里配有详细注释，覆盖从 WordCount 到状态容错、窗口、时间语义、CEP、Table API / SQL 的完整知识链路。

- Flink 版本：**1.18.1**
- JDK：**17**（已在你的机器上验证：OpenJDK 17.0.2）
- 构建工具：Maven 3.10
- 依赖：全部示例**不需要 Kafka、MySQL 等任何外部组件**，直接可跑

---

## 一、快速开始

```bash
cd flink-learning

# 方式一：菜单式运行（推荐）
./run.sh        # 选 2 会自动启动 socket 服务端，无需手动准备数据

# 方式二：直接运行某个示例
mvn -q compile                                                        # 首次编译
mvn exec:exec -Dexec.mainClass=com.example.flink.chapter01.BatchWordCount
```

> 说明：示例以**本地 MiniCluster** 方式运行，无需安装 Flink 集群。
> 部分示例是**无界流**（会一直运行），观察一段时间后按 `Ctrl + C` 停止。
> 若想在浏览器中查看算子链、并行度、反压等信息，可访问本地 Web UI：<http://localhost:8081>

---

## 二、学习路线与示例清单

| 章节 | 主题 | 示例类 | 关键知识点 |
|:---:|---|---|---|
| 1 | 快速上手 | `chapter01.BatchWordCount` | 批流统一、`setRuntimeMode(BATCH)`、flatMap/keyBy/sum |
| 1 | 流式 WordCount | `chapter01.StreamWordCount` | Socket Source、有状态聚合的持续输出（`./run.sh 2` 会自动起服务端并模拟数据） |
| 1 | 自定义 Source | `chapter01.StreamWordCountFromSource` | 自定义数据源统计 URL 的 PV |
| 2 | 数据源 | `chapter02.SourceDemo` | 集合/文件/自定义 Source、并行度 |
| 2 | 转换算子 | `chapter02.TransformDemo` | map/flatMap/filter、keyBy+sum/maxBy/reduce、rebalance/broadcast |
| 2 | 数据输出 | `chapter02.SinkDemo` | print、自定义 `RichSinkFunction`（open/invoke/close 生命周期） |
| 3 | 窗口类型 | `chapter03.window.WindowTypeDemo` | 滚动 / 滑动 / 会话 / 计数窗口的区别 |
| 3 | 窗口函数 | `chapter03.window.WindowFunctionDemo` | `AggregateFunction` 增量聚合 + `ProcessWindowFunction` 全窗口函数 |
| 4 | 时间语义与水位线 | `chapter04.time.WatermarkDemo` | EventTime、水位线生成与传播、窗口触发时机 |
| 4 | 迟到数据 | `chapter04.time.AllowedLatenessDemo` | `allowedLateness` + 侧输出流兜底 |
| 5 | 状态管理 | `chapter05.state.KeyedStateDemo` | ValueState / ListState / MapState 的用法与按 key 隔离 |
| 5 | 检查点与容错 | `chapter05.state.CheckpointDemo` | Checkpoint 配置、状态后端、重启策略、Exactly Once |
| 6 | 多流转换 | `chapter06.multistream.MultiStreamDemo` | union、connect + CoProcessFunction、Window Join、Interval Join |
| 7 | 侧输出流 | `chapter07.processfunction.SideOutputDemo` | ProcessFunction 分流（主流/支付流/加购流） |
| 7 | 定时器 | `chapter07.processfunction.TimerDemo` | 事件时间定时器 + 状态清理（手写窗口/超时检测） |
| 8 | Table API | `chapter08.table.TableApiDemo` | DataStream ↔ Table 互转、changelog 流 |
| 8 | Flink SQL | `chapter08.table.SqlDemo` | 临时视图、GROUP BY 聚合、窗口 TVF、TopN |
| 9 | CEP | `chapter09.cep.LoginFailDetectDemo` | 模式定义、量词、近邻关系、within 时间限制 |

### 目录结构

```
flink-learning
├── pom.xml
├── run.sh                        # 一键运行脚本
├── README.md
└── src/main
    ├── java/com/example/flink
    │   ├── common
    │   │   ├── model/Event.java              # 通用事件模型
    │   │   └── source/ClickSource.java       # 自定义并行 Source（含乱序数据）
    │   │   └── source/BoundedOutOfOrderSource.java  # 有界乱序 Source（时间语义演示）
    │   ├── chapter01/…                       # 快速上手
    │   ├── chapter02/…                       # DataStream 基础
    │   ├── chapter03/window/…                # 窗口
    │   ├── chapter04/time/…                  # 时间与水位线
    │   ├── chapter05/state/…                 # 状态与容错
    │   ├── chapter06/multistream/…           # 多流转换
    │   ├── chapter07/processfunction/…       # 底层处理函数
    │   ├── chapter08/table/…                 # Table API & SQL
    │   └── chapter09/cep/…                   # 复杂事件处理
    └── resources/log4j2.properties           # 日志配置（已降低框架日志噪音）
```

---

## 三、几个建议重点观察的实验

### 1. 水位线如何驱动窗口触发

```bash
./run.sh 9      # WatermarkDemo
```

数据源事件时间依次为 `1s、2s、12s、8s、13s、9s`，乱序容忍 2 秒。你会看到：

- 12s 数据到达后水位线推进到 10s，**立即触发** `[0,10)` 窗口（窗口内有 1s、2s 两条数据）
- 随后到达的 8s、9s 数据属于已触发的窗口，默认**被直接丢弃**

### 2. 迟到数据的两种补救手段

```bash
./run.sh 10     # AllowedLatenessDemo
```

同样的输入，窗口结果会依次输出 `2 → 3 → 4 条`：因为 `allowedLateness` 让窗口状态在触发后继续保留，
迟到数据到达时会再次触发窗口输出修正后的结果；超过保留期的极端迟到数据则进入侧输出流 `late-event`。

### 3. 有状态计算 + Checkpoint

```bash
./run.sh 12     # CheckpointDemo
```

Checkpoint 文件默认写在 `file:///tmp/flink-learning/checkpoints`，作业取消后仍保留，可用于手动恢复：

```bash
flink run -s file:///tmp/flink-learning/checkpoints/chk-1 \
  target/flink-learning-1.0-SNAPSHOT.jar
```

---

## 四、常见问题

**1）用 IDE 直接运行 main 方法报错（JDK 17 模块访问限制）**

在「Run/Debug Configurations → VM options」中加入：

```
--add-exports=java.base/sun.net.util=ALL-UNNAMED
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
--add-opens=java.base/java.io=ALL-UNNAMED
--add-opens=java.base/java.net=ALL-UNNAMED
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
```

> `mvn exec:exec` 已在 `pom.xml` 中自动带上这些参数，无需手动添加。

**2）Socket 示例（第 2 个）怎么玩**

- **最省事**：直接 `./run.sh 2`，脚本会自动启动 socket 服务端并循环模拟发送数据，你只需要看输出。
- **手动输入**：在一个终端执行 `nc -lk 7777`，然后**就在该终端里输入单词并回车**，
  再运行作业即可。注意 `nc -l` 同一时刻只服务一个连接（该连接被 Flink 占用），
  所以不要再开一个终端执行 `nc localhost 7777`，那样连不上。
- 想手动给脚本自动启动的服务端发数据：`echo "hello flink" > /tmp/flink_socket_input`
- 没有 `nc` 命令：`brew install netcat`；或直接运行第 3 个示例（`StreamWordCountFromSource`），零外部依赖。

**3）端口 8081 被占用**

两个示例不能同时运行（Web UI 端口冲突）。换端口可在代码中加：

```java
Configuration conf = new Configuration();
conf.setInteger("rest.port", 8082);
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
```

**4）打包提交到真实集群**

```bash
mvn clean package          # 生成 target/flink-learning-1.0-SNAPSHOT.jar
flink run target/flink-learning-1.0-SNAPSHOT.jar
```

---

## 五、下一步扩展方向

本项目为了让示例零依赖可跑，刻意没有引入外部系统。掌握上面 9 章后可继续扩展：

| 方向 | 需要的依赖 | 说明 |
|---|---|---|
| Kafka 连接器 | `flink-connector-kafka` | 生产最常用的 Source / Sink，配合 Checkpoint 实现端到端精确一次 |
| 文件连接器 | `flink-connector-files` | `FileSink` 支持分桶写入与精确一次语义（Parquet/ORC） |
| CDC 实时同步 | `flink-connector-mysql-cdc` | 捕获 MySQL binlog，实现实时数仓入湖 |
| RocksDB 状态后端 | `flink-statebackend-rocksdb` | 超大状态场景，支持增量 Checkpoint |
| JDBC Sink | `flink-connector-jdbc` | 写入 MySQL / ClickHouse 等 OLAP 库 |

添加依赖后重启 IDE 导入 Maven 变更即可开始实验。

---

## 六、推荐学习顺序

```
第1章 跑通 WordCount（建立整体印象）
  ↓
第2章 掌握 Source / Transform / Sink（能写完整的 DataStream 作业）
  ↓
第3章 窗口（流计算的核心抽象）
  ↓
第4章 时间语义 + 水位线（Flink 最难也最重要的概念）
  ↓
第5章 状态 + Checkpoint（理解 Flink 为何能做到精确一次）
  ↓
第6-7章 多流关联、ProcessFunction（应对复杂业务逻辑）
  ↓
第8章 Table API / SQL（实际生产最常用，开发效率最高）
  ↓
第9章 CEP（风控、实时告警等模式匹配场景）
```
