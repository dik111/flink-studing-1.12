package org.example.state;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Iterator;

/**
 * Desription: 使用operator中的listState模拟kafkaSource进行offset维护
 *
 * @ClassName StateDemo01_KeyedState
 * @Author Zhanyuwei
 * @Date 2021/2/11 20:54
 * @Version 1.0
 **/
public class StateDemo02_OperatorSate {

    public static void main(String[] args) throws Exception {
        // 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);
        //先直接使用下面的代码设置Checkpoint时间间隔和磁盘路径
        env.enableCheckpointing(1000);//每隔1s执行一次Checkpoint
        env.setStateBackend(new FsStateBackend("file:///E:/ckp"));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //固定延迟重启策略: 程序出现异常的时候，重启2次，每次延迟3秒钟重启，超过2次，程序退出
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 3000));


        // 1.source
        DataStreamSource<String> ds = env.addSource(new MyKafkaSource()).setParallelism(1);

        // 2.transformation


        // 3.sink
        ds.print();
        // 4.execute

        env.execute();
    }

    // 使用OperatorState中的ListState模拟kafkaSource进行offset维护
    public static class MyKafkaSource extends RichParallelSourceFunction<String> implements CheckpointedFunction{
        private boolean flag = true;

        // 用来存放offset
        private ListState<Long> offsetState =null;
        // 用来存放offset的值
        private Long offset = 0L;



        // 初始化/创建ListState
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<Long> stateDescriptor = new ListStateDescriptor<>("offsetState", Long.class);
            offsetState =  context.getOperatorStateStore().getListState(stateDescriptor);
        }

        // 使用state
        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {
            while (flag){

                Iterator<Long> iterator = offsetState.get().iterator();
                if (iterator.hasNext()){
                    offset = iterator.next();
                }
                offset += 1;

                int subTaskId = getRuntimeContext().getIndexOfThisSubtask();
                sourceContext.collect("subTaskId:"+subTaskId+", 当前的offset值为："+ offset);
                Thread.sleep(1000);
                // 模拟异常
                if (offset%5 ==0){
                    throw new Exception("bug出现了.....");
                }
            }
        }

        // 该方法会定时执行,将state状态从内存存入checkPoint中
        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            // 清理内容数据并存入checkpoint磁盘中
            offsetState.clear();
            offsetState.add(offset);
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }
}
