package org.example.metrics;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Desription: 在Map算子中提供一个Counter,统计map处理的数据条数，运行之后再WebUI上进行监控
 *
 * @ClassName MetricsDemo
 * @Author Zhanyuwei
 * @Date 2021/2/18 15:55
 * @Version 1.0
 **/
public class MetricsDemo {

    public static void main(String[] args) throws Exception {
        // 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // 1.source
        DataStream<String> lines = env.socketTextStream("localhost", 9999);

        // 2.transformation
        DataStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public void flatMap(String value, Collector<String> collector) throws Exception {
                String[] arr = value.split(" ");
                for (String word : arr) {
                    collector.collect(word);
                }
            }
        });

        DataStream<Tuple2<String, Integer>> wordAndOne = words.map(new RichMapFunction<String, Tuple2<String, Integer>>() {
            // 用来记录map处理了多少条数据
            Counter myCounter;

            // 对counter进行初始化
            @Override
            public void open(Configuration parameters) throws Exception {
                myCounter= getRuntimeContext().getMetricGroup().addGroup("myGroup").counter("myCounter");
            }


            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                myCounter.inc(); // 计算器+1
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
