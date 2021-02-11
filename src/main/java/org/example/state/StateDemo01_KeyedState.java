package org.example.state;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * Desription: 使用keyState中的valueState获取流数据中的最大值
 *
 * @ClassName StateDemo01_KeyedState
 * @Author Zhanyuwei
 * @Date 2021/2/11 20:54
 * @Version 1.0
 **/
public class StateDemo01_KeyedState {

    public static void main(String[] args) throws Exception {
        // 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // 1.source
        DataStream<Tuple2<String, Long>> tupleDS = env.fromElements(
                Tuple2.of("北京", 1L),
                Tuple2.of("上海", 2L),
                Tuple2.of("北京", 6L),
                Tuple2.of("上海", 8L),
                Tuple2.of("北京", 3L),
                Tuple2.of("上海", 4L)
        );

        // 2.transformation
        // 需求：求各个城市的value最大值
        // 实际中使用maxBy即可
        SingleOutputStreamOperator<Tuple2<String, Long>> result1 = tupleDS.keyBy(t -> t.f0).maxBy(1);

        // 学习时可以使用keyState中的valueState来实现
        SingleOutputStreamOperator<Tuple3<String, Long, Long>> result2 = tupleDS.keyBy(t -> t.f0).map(new RichMapFunction<Tuple2<String, Long>, Tuple3<String, Long, Long>>() {
            // 定义一个最大值用来存放最大值
            private ValueState<Long> maxValueState;

            // 状态初始化


            @Override
            public void open(Configuration parameters) throws Exception {
                // 创建状态描述器，初始化状态
                ValueStateDescriptor<Long> stateDescriptor = new ValueStateDescriptor<>("maxValueState", Long.class);
                // 根据状态描述器获取/初始化状态
                maxValueState = getRuntimeContext().getState(stateDescriptor);

            }

            @Override
            public Tuple3<String, Long, Long> map(Tuple2<String, Long> value) throws Exception {
                Long currentValue = value.f1;
                // 获取状态
                Long historyValue = maxValueState.value();
                // 判断状态
                if (historyValue == null || currentValue > historyValue) {
                    historyValue = currentValue;


                }
                // 更新状态
                maxValueState.update(historyValue);
                return Tuple3.of(value.f0, currentValue, historyValue);
            }
        });

        // 3.sink
        //result1.print();
        result2.print();
        // 4.execute

        env.execute();
    }
}
