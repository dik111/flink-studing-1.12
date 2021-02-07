package org.example.transformation;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Desription:transformation-rebalance-重平衡分区
 *
 * @ClassName TransformationDemo01
 * @Author Zhanyuwei
 * @Date 2021/2/6 15:48
 * @Version 1.0
 **/
public class TransformationDemo04 {

    public static void main(String[] args) throws Exception {
        // 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // source
        DataStream<Long> longDS = env.fromSequence(0, 100);

        // transformation
        DataStream<Long> filterDS = longDS.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long num) throws Exception {
                return num > 10;
            }
        });
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> result1 = filterDS.map(new RichMapFunction<Long, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(Long aLong) throws Exception {
                // 子任务id(分区编号)
                int subtaskId = getRuntimeContext().getIndexOfThisSubtask();

                return Tuple2.of(subtaskId, 1);
            }
        }).keyBy(t -> t.f0)
                .sum(1);

        // 调用了rebalance解决了数据倾斜
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> result2 = filterDS.rebalance().map(new RichMapFunction<Long, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(Long aLong) throws Exception {
                // 子任务id(分区编号)
                int subtaskId = getRuntimeContext().getIndexOfThisSubtask();

                return Tuple2.of(subtaskId, 1);
            }
        }).keyBy(t -> t.f0)
                .sum(1);


        // sink


        env.execute();


    }
}
