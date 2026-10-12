#!/bin/bash
# Flink 学习示例一键运行脚本
# 用法：
#   ./run.sh          # 显示菜单并选择
#   ./run.sh 3        # 直接运行第 3 个示例
#
# 注意：用 sh run.sh 或 ./run.sh 均可。

cd "$(dirname "$0")" || exit 1

NC_PID=""
FEEDER_PID=""
SOCKET_PORT=7777
FIFO=/tmp/flink_socket_input

# 脚本退出时清理后台启动的 nc 与数据发送器
cleanup() {
  [ -n "$NC_PID" ] && kill "$NC_PID" 2>/dev/null
  [ -n "$FEEDER_PID" ] && kill "$FEEDER_PID" 2>/dev/null
  pkill -f "nc -lk $SOCKET_PORT" 2>/dev/null
  pkill -f "flink_socket_input" 2>/dev/null
  rm -f "$FIFO"
}
trap cleanup EXIT INT TERM

# 判断端口是否已有服务监听
port_in_use() {
  nc -z localhost "$SOCKET_PORT" >/dev/null 2>&1
}

# 为第 2 个示例准备 socket 数据端：
# nc -l 同一时刻只能服务一个连接（Flink 会占用），所以不能在另一个终端再连进来输入。
# 这里用命名管道 FIFO 作为 nc 的输入，并自动循环写入示例句子，模拟真实的无界数据流；
# 你也可以随时手动发送：echo "hello flink" > /tmp/flink_socket_input
ensure_socket_listener() {
  if ! command -v nc >/dev/null 2>&1; then
    echo ">>> 未找到 nc 命令，无法自动启动监听。"
    echo ">>> 请先执行：brew install netcat，或改用第 3 个示例（无需外部依赖）：./run.sh 3"
    exit 1
  fi

  if port_in_use; then
    echo ">>> 检测到 $SOCKET_PORT 端口已有服务监听，请在那个终端里直接输入文本"
    return
  fi

  rm -f "$FIFO"
  mkfifo "$FIFO"

  # 服务端：持续从 FIFO 读取数据并发送给已连接的客户端（Flink）
  nohup sh -c "while true; do cat $FIFO; done | nc -lk $SOCKET_PORT" >/dev/null 2>&1 &
  NC_PID=$!

  # 客户端数据：循环产生句子，模拟无界流
  nohup sh -c 'while true; do
      echo "hello flink"
      sleep 1
      echo "hello world"
      sleep 1
      echo "flink is fast and scalable"
      sleep 1
      echo "hello spark flink"
      sleep 1
    done' >"$FIFO" 2>/dev/null &
  FEEDER_PID=$!

  sleep 2
  if port_in_use; then
    echo ">>> 已自动启动 socket 服务端：nc -lk $SOCKET_PORT（并自动模拟发送数据）"
    echo ">>> 也可手动发送：echo \"hello flink\" > $FIFO"
  else
    echo ">>> 启动 socket 服务端失败，请手动在一个终端执行：nc -lk $SOCKET_PORT 后再重试"
    exit 1
  fi
}

run() {
  echo ">>> 运行：$2"
  mvn -B -q exec:exec -Dexec.mainClass="$1"
}

show_menu() {
  cat <<'EOF'
请选择要运行的示例（输入编号）：

  1) 第1章 批处理 WordCount
  2) 第1章 流式 WordCount（Socket，自动启动监听）
  3) 第1章 流式 WordCount（自定义 Source，无需外部依赖）
  4) 第2章 Source 数据源
  5) 第2章 Transform 转换算子
  6) 第2章 Sink 输出
  7) 第3章 窗口类型
  8) 第3章 窗口函数
  9) 第4章 水位线 Watermark
 10) 第4章 迟到数据处理
 11) 第5章 键控状态 State
 12) 第5章 检查点 Checkpoint
 13) 第6章 多流转换 union/connect/join
 14) 第7章 侧输出流 Side Output
 15) 第7章 定时器 Timer
 16) 第8章 Table API
 17) 第8章 Flink SQL
 18) 第9章 CEP 复杂事件处理

  0) 编译项目

EOF
  printf "编号: "
  read -r choice
}

if [ $# -eq 0 ]; then
  show_menu
else
  choice=$1
fi

case "$choice" in
  0)  mvn -B -q clean compile && echo ">>> 编译完成" ;;
  1)  run com.example.flink.chapter01.BatchWordCount "批处理 WordCount" ;;
  2)  ensure_socket_listener
      run com.example.flink.chapter01.StreamWordCount "流式 WordCount(Socket)" ;;
  3)  run com.example.flink.chapter01.StreamWordCountFromSource "流式 WordCount(自定义 Source)" ;;
  4)  run com.example.flink.chapter02.SourceDemo "Source 数据源" ;;
  5)  run com.example.flink.chapter02.TransformDemo "Transform 转换算子" ;;
  6)  run com.example.flink.chapter02.SinkDemo "Sink 输出" ;;
  7)  run com.example.flink.chapter03.window.WindowTypeDemo "窗口类型" ;;
  8)  run com.example.flink.chapter03.window.WindowFunctionDemo "窗口函数" ;;
  9)  run com.example.flink.chapter04.time.WatermarkDemo "水位线 Watermark" ;;
  10) run com.example.flink.chapter04.time.AllowedLatenessDemo "迟到数据处理" ;;
  11) run com.example.flink.chapter05.state.KeyedStateDemo "键控状态 State" ;;
  12) run com.example.flink.chapter05.state.CheckpointDemo "检查点 Checkpoint" ;;
  13) run com.example.flink.chapter06.multistream.MultiStreamDemo "多流转换" ;;
  14) run com.example.flink.chapter07.processfunction.SideOutputDemo "侧输出流" ;;
  15) run com.example.flink.chapter07.processfunction.TimerDemo "定时器 Timer" ;;
  16) run com.example.flink.chapter08.table.TableApiDemo "Table API" ;;
  17) run com.example.flink.chapter08.table.SqlDemo "Flink SQL" ;;
  18) run com.example.flink.chapter09.cep.LoginFailDetectDemo "CEP 复杂事件处理" ;;
  *)  echo "无效编号：$choice" ; exit 1 ;;
esac
