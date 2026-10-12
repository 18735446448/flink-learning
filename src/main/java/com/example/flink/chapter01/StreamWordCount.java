package com.example.flink.chapter01;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * 第 1 章：流式 WordCount —— 从 Socket 读取无界数据流，实时统计词频。
 *
 * <p>运行方式：
 * <pre>
 *   # 终端 A：启动数据发送端
 *   nc -lk 7777                 # macOS / Linux（没有 nc 可用 brew install netcat）
 *   # 终端 B：启动作业（或执行 ./run.sh 2，脚本会自动帮你起监听）
 *   mvn exec:exec -Dexec.mainClass=com.example.flink.chapter01.StreamWordCount
 *   # 终端 C：输入文本并回车，即可看到实时词频统计
 *   nc localhost 7777
 * </pre>
 *
 * <p>也可以在 IDE 中通过 Program arguments 自定义地址（格式：host port，例如 localhost 7777）。
 *
 * <p>核心知识点：
 * <ul>
 *   <li>流模式下聚合算子有状态：同一个 key 每来一条数据就输出一次「当前累计值」</li>
 *   <li>程序不会自动结束，属于常驻作业（streaming job）</li>
 *   <li>socketTextStream 并行度为 1，仅适合学习演示，生产请用 Kafka 等可重放的 Source</li>
 * </ul>
 */
public class StreamWordCount {

    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 7777;
    /** 连接失败后的重试次数，每 5 秒重试一次，留出启动 nc 的时间 */
    private static final long MAX_RETRY = 12;

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : DEFAULT_HOST;
        int port = args.length > 1 ? Integer.parseInt(args[1]) : DEFAULT_PORT;

        // 启动前先做一次连通性检查：没连上就给出清晰的中文提示，而不是抛一大段堆栈
        if (!isSocketAvailable(host, port)) {
            printGuide(host, port);
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从 socket 读取无界文本流（第 4 个参数 = 失败重试次数）
        DataStream<String> lineStream = env.socketTextStream(host, port, "\n", MAX_RETRY);

        DataStream<Tuple2<String, Integer>> result = lineStream
                .flatMap((String line, Collector<String> out) -> {
                    for (String word : line.split("\\s+")) {
                        if (!word.isEmpty()) {
                            out.collect(word);
                        }
                    }
                })
                .returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(t -> t.f0)
                .sum(1);

        result.print("stream-word-count");

        env.execute("Streaming WordCount (socket)");
    }

    /** 判断目标端口是否已经有服务在监听 */
    private static boolean isSocketAvailable(String host, int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 2000);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    /** 连接不上时打印操作指引 */
    private static void printGuide(String host, int port) {
        System.out.println();
        System.out.println("=============================================================");
        System.out.println("无法连接到 " + host + ":" + port + "，本示例需要一个 socket 数据发送端。");
        System.out.println();
        System.out.println("请新开一个终端执行：  nc -lk " + port);
        System.out.println("（没有 nc 命令可先执行： brew install netcat ）");
        System.out.println();
        System.out.println("启动后再新开一个终端发送数据： nc " + host + " " + port);
        System.out.println("然后输入任意单词并回车，即可看到实时的词频统计结果。");
        System.out.println();
        System.out.println("如果不想安装 nc，请改用第 1 章第 3 个示例（无需外部依赖）：");
        System.out.println("  ./run.sh 3    或    mvn exec:exec "
                + "-Dexec.mainClass=com.example.flink.chapter01.StreamWordCountFromSource");
        System.out.println();
        System.out.println("接下来会持续重试连接（约 60 秒），你现在去启动 nc 也来得及。");
        System.out.println("=============================================================");
        System.out.println();
    }
}
