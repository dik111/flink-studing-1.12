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

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Desription: 演示Flink table & SQL案例--将DataStream数据转Table或View然后使用SQL进行统计查询
 *
 * @ClassName SQLDemo01
 * @Author Zhanyuwei
 * @Date 2021/2/16 16:51
 * @Version 1.0
 **/
public class SQLDemo01 {

    public static void main(String[] args) throws Exception {
        // env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);

        // source
        DataStream<Order> orderA = env.fromCollection(Arrays.asList(
                new Order(1L, "beer", 3),
                new Order(1L, "diaper", 4),
                new Order(3L, "rubber", 2)));

        DataStream<Order> orderB = env.fromCollection(Arrays.asList(
                new Order(2L, "pen", 3),
                new Order(2L, "rubber", 3),
                new Order(4L, "beer", 1)));


        // transformation --将DataStream数据转Table或View
        Table tableA = tenv.fromDataStream(orderA, $("user"), $("product"), $("amount"));
        tenv.createTemporaryView("tableB",orderB,$("user"), $("product"), $("amount"));

        // 查询：tableA中amount>2的和tableB中amount>1的数值，最后合并
        /**
         * select * from tableA where amount >2
         * union
         * select * from tableB where amount >1
         */
        String sql = "select * from  "+tableA+"  where amount >2 \n" +
                "union all \n" +
                "select * from tableB where amount >1";
        Table resultTable = tenv.sqlQuery(sql);

        // 将Table转为DataStream
        // sink
        //DataStream<Order> resultDS = tenv.toAppendStream(resultTable, Order.class);
        DataStream<Tuple2<Boolean, Order>> resultDS = tenv.toRetractStream(resultTable, Order.class);
        resultDS.print();

        //execute
        env.execute();

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order {
        public Long user;
        public String product;
        public int amount;
    }

}
