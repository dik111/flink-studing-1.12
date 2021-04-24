package org.example.cdc;


import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import com.mchange.v2.util.PropertiesUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


import java.util.Properties;

/**
 * Desription:
 *
 * @ClassName FlinkCDC
 * @Author Zhanyuwei
 * @Date 2021/4/24 3:09 下午
 * @Version 1.0
 **/
public class FlinkCDC {

    public static void main(String[] args) throws Exception {

        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        // 2.checkpoint
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //设置是否清理检查点,表示 Cancel 时是否需要保留当前的 Checkpoint，默认 Checkpoint会在作业被Cancel时被删除
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        if (SystemUtils.IS_OS_WINDOWS) {
            env.setStateBackend(new FsStateBackend("file:///D:\\ckp"));
        }
        else if (SystemUtils.IS_OS_MAC){
            env.setStateBackend(new FsStateBackend("file:////Users/yuwei1/Downloads/ckp"));
        }
        else {
            env.setStateBackend(new FsStateBackend("hdfs://master:8020/flink/cdc/ck"));
        }
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2,2000L));
        System.setProperty("HADOOP_USER_NAME","hdfs");

        // 3.创建mysql cdc source
        Properties properties = new Properties();
        properties.setProperty("scan.startup.mode","initial");
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("10.10.19.235")
                .port(3306)
                .username("root")
                .password("asd2828")
                .databaseList("example")
                .tableList("example.z_user_info")
                .deserializer(new StringDebeziumDeserializationSchema())
                .debeziumProperties(properties)
                .build();
        // 4.读取MySQL数据
        DataStreamSource<String> streamSource = env.addSource(sourceFunction);

        // 5.打印
        streamSource.print();

        // 6.执行任务
        env.execute();
    }
}
