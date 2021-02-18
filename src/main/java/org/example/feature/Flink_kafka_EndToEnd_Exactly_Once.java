package org.example.feature;

import org.apache.commons.lang3.SystemUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.Collector;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Desription:
 *
 * @ClassName Flink_kafka_EndToEnd_Exactly_Once
 * @Author Zhanyuwei
 * @Date 2021/2/17 22:01
 * @Version 1.0
 **/
public class Flink_kafka_EndToEnd_Exactly_Once {
    public static void main(String[] args) throws Exception {

        // env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(5, TimeUnit.SECONDS)));
        // 开启checkpoint
        env.enableCheckpointing(1000);
        if (SystemUtils.IS_OS_WINDOWS) {
            env.setStateBackend(new FsStateBackend("file:///E:\\ckp"));
        } else {
            env.setStateBackend(new FsStateBackend("hdfs://master:8020/flink-checkpoint/checkpoint"));
        }
        //===========类型2:建议参数===========
        //设置两个Checkpoint 之间最少等待时间,如设置Checkpoint之间最少是要等 500ms(为了避免每隔1000ms做一次Checkpoint的时候,前一次太慢和后一次重叠到一起去了)
        //如:高速公路上,每隔1s关口放行一辆车,但是规定了两车之前的最小车距为500m
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);//默认是0
        //设置如果在做Checkpoint过程中出现错误，是否让整体任务失败：true是  false不是
        //env.getCheckpointConfig().setFailOnCheckpointingErrors(false);//默认是true
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);//默认值为0，表示不容忍任何检查点失败
        //设置是否清理检查点,表示 Cancel 时是否需要保留当前的 Checkpoint，默认 Checkpoint会在作业被Cancel时被删除
        //ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION：true,当作业被取消时，删除外部的checkpoint(默认值)
        //ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION：false,当作业被取消时，保留外部的checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //===========类型3:直接使用默认的即可===============
        //设置checkpoint的执行模式为EXACTLY_ONCE(默认)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //设置checkpoint的超时时间,如果 Checkpoint在 60s内尚未完成说明该次Checkpoint失败,则丢弃。
        env.getCheckpointConfig().setCheckpointTimeout(60000);//默认10分钟
        //设置同一时间有多少个checkpoint可以同时执行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);//默认为1

        // source
        Properties props  = new Properties();
        props.setProperty("bootstrap.servers", "slave01:9092");
        props.setProperty("group.id", "flink");
        // 有offset记录从记录位置开始消费，
        // latest-->没有记录从最新的或最后的消息开始消费
        // earliest-->有offset记录从记录位置开始消费，没有记录从最早的或最开始的消息开始消费
        props.setProperty("auto.offset.reset","latest");
        //会开启一个后台线程每隔5s检测一下Kafka的分区情况，实现动态分区检测
        props.setProperty("flink.partition-discovery.interval-millis","5000");
        // 自动提交(提交到默认主题)
        //props.setProperty("enable.auto.commit", "true");
        // 自动提交的时间
        //props.setProperty("auto.commit.interval.ms", "2000");
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<String>("flink_kafka1", new SimpleStringSchema(), props);
        // 在做checkpoint的时候提交offset
        kafkaSource.setCommitOffsetsOnCheckpoints(true);

        // 使用kafkaSource
        DataStream<String> kafkaDS = env.addSource(kafkaSource);

        // transformation
        SingleOutputStreamOperator<String> result = kafkaDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            private Random random = new Random();

            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] arr = value.split(" ");
                for (String word : arr) {
                    int num = random.nextInt(5);
                    if (num >3){
                        System.out.println("随机异常产生了");
                        throw new Exception("随机异常产生了");
                    }
                    out.collect(Tuple2.of(word, 1));
                }
            }
        }).keyBy(t -> t.f0)
                .sum(1)
                .map(new MapFunction<Tuple2<String, Integer>, String>() {
                    @Override
                    public String map(Tuple2<String, Integer> value) throws Exception {
                        return value.f0 + ":" + value.f1;
                    }
                });

        // sink
        Properties props2 = new Properties();
        props2.setProperty("bootstrap.servers", "slave01:9092");
        props2.setProperty("transaction.timeout.ms", 1000 * 5 + "");//设置事务超时时间，也可在kafka配置中设置
        FlinkKafkaProducer<String> kafkaSink = new FlinkKafkaProducer<>(
                "flink_kafka2",
                new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()),
                props2,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        result.addSink(kafkaSink);


        // execute
        env.execute();
    }
}
