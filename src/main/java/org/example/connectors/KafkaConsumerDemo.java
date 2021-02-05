package org.example.connectors;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * Desription:
 *
 * @ClassName KafkaConsumerDemo
 * @Author Zhanyuwei
 * @Date 2021/2/5 5:41 下午
 * @Version 1.0
 **/
public class KafkaConsumerDemo {

    public static void main(String[] args) throws Exception {
        // 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // source
        Properties props  = new Properties();
        props.setProperty("bootstrap.servers", "master:9092");
        props.setProperty("group.id", "flink");
        // 有offset记录从记录位置开始消费，
        // latest-->没有记录从最新的或最后的消息开始消费
        // earliest-->有offset记录从记录位置开始消费，没有记录从最早的或最开始的消息开始消费
        props.setProperty("auto.offset.reset","latest");
        //会开启一个后台线程每隔5s检测一下Kafka的分区情况，实现动态分区检测
        props.setProperty("flink.partition-discovery.interval-millis","5000");
        // 自动提交(提交到默认主题)
        props.setProperty("enable.auto.commit", "true");
        // 自动提交的时间
        props.setProperty("auto.commit.interval.ms", "2000");
        FlinkKafkaConsumer kafkaSource = new FlinkKafkaConsumer<String>("flink_kafka", new SimpleStringSchema(), props);

        // 使用kafkaSource
        DataStream kafkaDS = env.addSource(kafkaSource);

        // transformation
        SingleOutputStreamOperator etlDS = kafkaDS.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return value.contains("success");
            }
        });



        // sink
        etlDS.print();
        Properties props2 = new Properties();
        props2.setProperty("bootstrap.servers", "master:9092");
        etlDS.addSink(new FlinkKafkaProducer<String>("flink_kafka2", new SimpleStringSchema(), props2));

        env.execute();

    }
}
