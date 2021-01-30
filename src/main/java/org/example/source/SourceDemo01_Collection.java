package org.example.source;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * Desription:演示DataStream-Source
 *
 * @ClassName SourceDemo01_Collection
 * @Author Zhanyuwei
 * @Date 2021/1/30 8:59 下午
 * @Version 1.0
 **/
public class SourceDemo01_Collection {

    public static void main(String[] args) throws Exception {
        // 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // 1.source
        DataStream<String> ds1 = env.fromElements("itcast hadoop spark", "itcast hadoop spark", "itcast hadoop", "itcast");
        DataStream<String> ds2 = env.fromCollection(Arrays.asList("itcast hadoop spark", "itcast hadoop spark", "itcast hadoop", "itcast"));
        DataStream<Long> ds3 = env.generateSequence(1, 100);
        DataStream<Long> ds4 = env.fromSequence(1, 100);


        // 2.transformation

        // 3.sink
        ds1.print();
        ds2.print();
        ds3.print();
        ds4.print();

        // 4.execute

        env.execute();
    }
}
