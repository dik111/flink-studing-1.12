package org.example.connectors;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.example.entity.Student;
import org.example.sink.SinkDemo02;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * Desription:
 *
 * @ClassName JDBCDemo
 * @Author Zhanyuwei
 * @Date 2021/2/4 8:49 下午
 * @Version 1.0
 **/
public class JDBCDemo {

    public static void main(String[] args) throws Exception {

        // 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // source
        DataStream<Student> studentDs = env.fromElements(new Student(null, "tony2", 18));



        // sink
        studentDs.addSink(JdbcSink.sink("insert into student(`id`,`name`,`age`) values (null ,?,?) ",(ps,value)->{
            ps.setString(1,value.getName());
            ps.setInt(2,value.getAge());
        },new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .withDriverName("com.mysql.jdbc.Driver")
        .withUrl("jdbc:mysql://127.0.0.1:3306/finedb?useSSL=false")
        .withUsername("root")
        .withPassword("asd2828")
        .build()));

        env.execute();
    }

}
