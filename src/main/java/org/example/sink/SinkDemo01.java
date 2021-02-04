package org.example.sink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Desription: 演示Datastream-sink-基于控制台和文件
 *
 * @ClassName SinkDemo01
 * @Author Zhanyuwei
 * @Date 2021/2/4 8:55 下午
 * @Version 1.0
 **/
public class SinkDemo01 {

    public static void main(String[] args) throws Exception {

        // 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // source
        DataStream<String> ds = env.readTextFile("src/main/resources/words.txt");

        // sink
        ds.print("输出标识");

        // 会在控制台上以红色输出
        ds.printToErr();
        ds.writeAsText("data/output/result1").setParallelism(1);
        ds.writeAsText("data/output/result2").setParallelism(2);


        env.execute();
    }
}
