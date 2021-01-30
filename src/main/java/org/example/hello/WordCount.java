package org.example.hello;


import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * Desription:
 *
 * @ClassName WordCount
 * @Author Zhanyuwei
 * @Date 2021/1/25 10:25 下午
 * @Version 1.0
 **/
public class WordCount {

    public static void main(String[] args) throws Exception {
        //ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        //DataSet<String> line = env.fromElements("itcast hadoop spark", "itca st hadoop spark", "itcast hadoop", "itcast");
        DataStream<String> lines = env.fromElements("itcast hadoop spark", "itcast hadoop spark", "itcast hadoop", "itcast");

        // 切割
        DataStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public void flatMap(String value, Collector<String> collector) throws Exception {
                String[] arr = value.split(" ");
                for (String word : arr) {
                    collector.collect(word);
                }
            }
        });
        
        // 记为1
        DataStream<Tuple2<String, Integer>> wordAndOne = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {

                return Tuple2.of(s, 1);
            }
        });


        // 分组
        KeyedStream<Tuple2<String, Integer>, String> grouped = wordAndOne.keyBy(t -> t.f0);

        // 聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = grouped.sum(1);

        result.print();

        env.execute();

    }
}
