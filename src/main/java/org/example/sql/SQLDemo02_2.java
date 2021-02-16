package org.example.sql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Desription: 演示Flink table & SQL案例--使用SQL和Table两种方式做WordCount
 *
 * @ClassName SQLDemo01
 * @Author Zhanyuwei
 * @Date 2021/2/16 16:51
 * @Version 1.0
 **/
public class SQLDemo02_2 {

    public static void main(String[] args) throws Exception {
        // env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);

        // source
        DataStream<WC> wordsDS = env.fromElements(
                new WC("Hello", 1),
                new WC("World", 1),
                new WC("Hello", 1)
        );



        // transformation
        // 将DataStream转为view或table
        //tenv.createTemporaryView("t_words",wordsDS,$("word"), $("frequency"));
        Table table = tenv.fromDataStream(wordsDS, $("word"), $("frequency"));

        // 使用table风格查询
        Table resultTable = table
                .groupBy($("word"))
                .select($("word"), $("frequency").sum().as("frequency"))
                .filter($("frequency").isEqual(2));



        // 转换为DataStream
        DataStream<Tuple2<Boolean, WC>> resultDS = tenv.toRetractStream(resultTable, WC.class);

        // sink
        resultDS.print();

        //execute
        env.execute();


    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Data
    public static class WC {
        public String word;
        public long frequency;
    }


}
