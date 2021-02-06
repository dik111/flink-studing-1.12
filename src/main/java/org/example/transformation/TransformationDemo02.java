package org.example.transformation;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

/**
 * Desription:transformation-合并和连接操作
 *
 * @ClassName TransformationDemo01
 * @Author Zhanyuwei
 * @Date 2021/2/6 15:48
 * @Version 1.0
 **/
public class TransformationDemo02 {

    public static void main(String[] args) throws Exception {
        // 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // source
        DataStream<String> ds1 = env.fromElements("hadoop", "spark", "flink");
        DataStream<String> ds2 = env.fromElements("hadoop", "spark", "flink");
        DataStream<Long> ds3 = env.fromElements(1L, 2L, 3L);

        // transformation
        DataStream<String> result1 = ds1.union(ds2);
        //ds1.union(ds3)
        ConnectedStreams<String, String> result2 = ds1.connect(ds2);
        ConnectedStreams<String, Long> result3 = ds1.connect(ds3);

        SingleOutputStreamOperator<String> result = result3.map(new CoMapFunction<String, Long, String>() {
            @Override
            public String map1(String s) throws Exception {
                return "string:" + s;
            }

            @Override
            public String map2(Long aLong) throws Exception {
                return "Long:" + aLong;
            }
        });
        // sink
        result1.print();
        result.print();

        env.execute();


    }
}
