package com.example.flink.chapter07.processfunction;

import com.example.flink.common.model.Event;
import com.example.flink.common.source.ClickSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 第 7 章：ProcessFunction —— Flink 的「底层万能算子」，以及侧输出流（Side Output）。
 *
 * <p>ProcessFunction 能做什么：
 * <ul>
 *   <li>访问每条数据的<b>时间戳</b>与<b>水位线</b>（ctx.timestamp() / ctx.timerService().currentWatermark()）</li>
 *   <li>注册<b>定时器</b>（第 7 章的 TimerDemo）</li>
 *   <li>把数据发到<b>侧输出流</b>，实现「一条流按规则拆成多条流」而无需过滤多次</li>
 * </ul>
 *
 * <p>侧输出流的典型场景：数据清洗时把脏数据单独收集、窗口迟到的兜底数据、慢/快数据分流等。
 * 注意主流与侧输出流的数据类型可以不同。
 */
public class SideOutputDemo {

    /** 支付事件流 */
    private static final OutputTag<Event> PAY_TAG = new OutputTag<Event>("pay") {
    };
    /** 加购事件流 */
    private static final OutputTag<Event> CART_TAG = new OutputTag<Event>("cart") {
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> mainStream = env
                .addSource(new ClickSource(800L))
                .process(new ProcessFunction<Event, Event>() {
                    @Override
                    public void processElement(Event event, Context ctx, Collector<Event> out) {
                        switch (event.getUrl()) {
                            case "/pay":
                                // 发送到侧输出流：支付事件
                                ctx.output(PAY_TAG, event);
                                break;
                            case "/cart":
                                ctx.output(CART_TAG, event);
                                break;
                            default:
                                // 主流：其它普通浏览事件
                                out.collect(event);
                        }
                    }
                });

        mainStream.print("main-normal");

        DataStream<Event> payStream = mainStream.getSideOutput(PAY_TAG);
        payStream.print("side-pay");

        DataStream<Event> cartStream = mainStream.getSideOutput(CART_TAG);
        cartStream.print("side-cart");

        env.execute("Side Output Demo");
    }
}
