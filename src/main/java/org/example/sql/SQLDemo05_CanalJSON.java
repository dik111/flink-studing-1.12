package org.example.sql;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Desription: 演示Flink table & SQL案例--从kafka:input_kafka主题消费数据并生成table,然后过滤出状态success的数据再写回到kafka:output_kafka
 *
 * @ClassName SQLDemo01
 * @Author Zhanyuwei
 * @Date 2021/2/16 16:51
 * @Version 1.0
 **/
public class SQLDemo05_CanalJSON {

    public static void main(String[] args) throws Exception {
        // env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);

        // source
        TableResult inputTable = tenv.executeSql(
                "CREATE TABLE example_test5_5 (\n" +
                        "  -- 元数据与 MySQL \"products\" 表完全相同\n" +
                        "  id STRING,\n" +
                        "  name STRING,\n" +
                        "  age STRING\n" +
                        ") WITH (\n" +
                        " 'connector' = 'kafka',\n" +
                        " 'topic' = 'example_test5_5',\n" +
                        " 'properties.bootstrap.servers' = 'slave01:9092',\n" +
                        " 'properties.group.id' = 'testGroup2',\n" +
                        " 'format' = 'canal-json',  -- 使用 canal-json 格式\n" +
                        "'canal-json.ignore-parse-errors' = 'true' \n"+
                        ")"
        );



        // transformation
        // 编写sql过滤出状态success的数据
        String sql = "select " +
                "id," +
                "left(name,3) as name," +
                "age " +
                "from example_test5_5 " ;

        Table etlResult = tenv.sqlQuery(sql);


        // sink
        //TableResult outputTable = tenv.executeSql(
        //        "CREATE TABLE output_kafka (\n" +
        //                "  `user_id` BIGINT,\n" +
        //                "  `page_id` BIGINT,\n" +
        //                "  `status` STRING\n" +
        //                ") WITH (\n" +
        //                "  'connector' = 'kafka',\n" +
        //                "  'topic' = 'output_kafka',\n" +
        //                "  'properties.bootstrap.servers' = 'slave01:9092',\n" +
        //                "  'format' = 'json',\n" +
        //                "  'sink.partitioner' = 'round-robin'\n" +
        //                ")"
        //);
        DataStream<Tuple2<Boolean, Row>> resultDS = tenv.toRetractStream(etlResult,Row.class);
        resultDS.print();



        //tenv.executeSql("insert into output_kafka select * from "+etlResult);

        //resultDS.print();

        //execute
        env.execute();


    }



}
