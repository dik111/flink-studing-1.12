package org.example.source;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;
import java.util.UUID;

/**
 * Desription:
 *
 * @ClassName SourceDemo04_Customer
 * @Author Zhanyuwei
 * @Date 2021/2/5 9:38 下午
 * @Version 1.0
 **/
public class SourceDemo04_Customer {

    public static void main(String[] args) throws Exception {

        // 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // source
        DataStream<Order> orderDs = env.addSource(new MyOrderSource()).setParallelism(2);

        // sink
        orderDs.print();


        env.execute();


    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Order{
        private String id;
        private Integer userId;
        private Integer money;
        private Long createTime;
    }

    public static class MyOrderSource extends RichParallelSourceFunction<Order>{

        private Boolean flag = true;

        // 执行并生成数据
        @Override
        public void run(SourceContext<Order> ctx) throws Exception {
            Random random = new Random();
            while (flag){
                String oid = UUID.randomUUID().toString();
                int userId = random.nextInt(3);
                int money = random.nextInt(101);
                long createTime = System.currentTimeMillis();
                ctx.collect(new Order(oid,userId,money,createTime));
                Thread.sleep(1000);
            }
        }

        // 执行cancel命令的时候执行
        @Override
        public void cancel() {
            flag = false;
        }
    }
}
