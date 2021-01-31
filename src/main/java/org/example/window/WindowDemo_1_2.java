package org.example.window;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.example.entity.CartInfo;

/**
 * Desription: 演示基于时间的滚动和滑动窗口
 *
 * @ClassName WindowDemo_1_2
 * @Author Zhanyuwei
 * @Date 2021/1/31 2:59 下午
 * @Version 1.0
 **/
public class WindowDemo_1_2 {

    public static void main(String[] args) throws Exception {
        // 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // 1.source
        DataStream<String> lines = env.socketTextStream("localhost", 9999);

        // 2.transformation
        SingleOutputStreamOperator<CartInfo> carDs = lines.map(new MapFunction<String, CartInfo>() {

            @Override
            public CartInfo map(String value) throws Exception {
                String[] arr = value.split(",");

                return new CartInfo(arr[0], Integer.parseInt(arr[1]));
            }
        });

        KeyedStream<CartInfo, String> keyedDs = carDs.keyBy(CartInfo::getSensorId);

        //SingleOutputStreamOperator<CartInfo> result1 = keyedDs.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
        //        .sum("count");

        SingleOutputStreamOperator<CartInfo> result2 = keyedDs.window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5)))
                .sum("count");

        //result1.print();
        result2.print();

        /*
        1,5
        2,5
        3,5
        4,5
         */

        env.execute();

    }
}
