package org.example.transformation;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


import java.io.OutputStream;

/**
 * Desription:transformation-拆分（split）和选择(select)操作
 * 注意split和select在flink1.12中已经过期并移除了
 * 所以得使用outPutTag和process来实现
 * 需求：对流中的数据按照奇数和偶数拆分并选择
 *
 * @ClassName TransformationDemo01
 * @Author Zhanyuwei
 * @Date 2021/2/6 15:48
 * @Version 1.0
 **/
public class TransformationDemo03 {

    public static void main(String[] args) throws Exception {
        // 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // source
        DataStreamSource<Integer> ds = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // transformation
        OutputTag<Integer> oddTag = new OutputTag<>("奇数", TypeInformation.of(Integer.class));
        OutputTag<Integer> evenTag = new OutputTag<>("偶数",TypeInformation.of(Integer.class));

        SingleOutputStreamOperator<Integer> result = ds.process(new ProcessFunction<Integer, Integer>() {
            @Override
            public void processElement(Integer value, Context context, Collector<Integer> collector) throws Exception {
                if (value % 2 == 0) {
                    context.output(evenTag, value);
                } else {
                    context.output(oddTag, value);
                }

            }
        });
        DataStream<Integer> oddResult = result.getSideOutput(oddTag);
        DataStream<Integer> evenResult = result.getSideOutput(evenTag);

        // sink
        oddResult.print("奇数");
        evenResult.print("偶数");


        env.execute();


    }
}
