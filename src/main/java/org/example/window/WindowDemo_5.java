package org.example.window;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.example.entity.CartInfo;

/**
 * Desription: 演示基于数量的滚动和滑动窗口
 *
 * @ClassName WindowDemo_1_2
 * @Author Zhanyuwei
 * @Date 2021/1/31 2:59 下午
 * @Version 1.0
 **/
public class WindowDemo_5 {

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

        //需求:设置会话超时时间为10s,10s内没有数据到来,则触发上个窗口的计算(前提是上一个窗口得有数据!)
        SingleOutputStreamOperator<CartInfo> result1 = keyedDs.window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
                .sum("count");

        result1.print();

        //result2.print();

        env.execute();

    }
}
