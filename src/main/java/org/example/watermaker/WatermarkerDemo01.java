package org.example.watermaker;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.example.entity.CartInfo;
import org.example.entity.Order;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;

/**
 * Desription: 演示基于事件时间的窗口计算+watermaker解决一定程度上的数据乱序/延迟达到的问题
 *
 * @ClassName WatermarkerDemo01
 * @Author Zhanyuwei
 * @Date 2021/2/10 4:02 下午
 * @Version 1.0
 **/
public class WatermarkerDemo01 {

    public static void main(String[] args) throws Exception {
        // 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // 1.source
        DataStreamSource<Order> orderDS = env.addSource(new SourceFunction<Order>() {

            private boolean flag = true;

            @Override
            public void run(SourceContext<Order> ctx) throws Exception {
                Random random = new Random();

                while (flag) {
                    String orderId = UUID.randomUUID().toString();
                    int userId = random.nextInt(2);
                    int money = random.nextInt(101);
                    // 随机模拟延迟
                    long eventTime = System.currentTimeMillis() - random.nextInt(5) * 1000;
                    ctx.collect(new Order(orderId, userId, money, eventTime));
                    Thread.sleep(1000);

                }
            }

            @Override
            public void cancel() {
                flag = false;
            }
        });

        // 2.transformation
        // 每隔5s计算最近5s的数据，求每个用户的订单总金额，要求:基于事件时间的窗口计算+watermaker
        SingleOutputStreamOperator<Order> orderDSWithWatermarker = orderDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<Order>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((order, timestamp) -> order.getEventTime()));

        SingleOutputStreamOperator<Order> result = orderDSWithWatermarker.keyBy(Order::getUserId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum("money");

        // 3.sink
        result.print();
        env.execute();

    }
}
