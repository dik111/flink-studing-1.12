package org.example.sink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.example.entity.Student;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * Desription: 演示Datastream-sink-自定义sink
 *
 * @ClassName SinkDemo01
 * @Author Zhanyuwei
 * @Date 2021/2/4 8:55 下午
 * @Version 1.0
 **/
public class SinkDemo02 {

    public static void main(String[] args) throws Exception {

        // 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // source
        DataStreamSource<Student> studentDs = env.fromElements(new Student(null, "tonyma", 18));



        // sink
        studentDs.addSink(new MySQLSink());

        env.execute();
    }

    public static class MySQLSink extends RichSinkFunction<Student> {
        private Connection conn = null;
        private PreparedStatement ps = null;


        @Override
        public void open(Configuration parameters) throws Exception {
            conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/finedb?useSSL=false","root","asd2828");
            String sql = "insert into student(`id`,`name`,`age`) values (null ,?,?) ";
            ps = conn.prepareStatement(sql);
        }



        @Override
        public void invoke(Student value, Context context) throws Exception {
            // 设置?占位符参数值
            ps.setString(1,value.getName());
            ps.setInt(2,value.getAge());

            // 执行sql
            ps.executeUpdate();
        }

        @Override
        public void close() throws Exception {
            if (conn != null){
                conn.close();
            }
            if (ps != null){
                ps.close();
            }
        }
    }
}
