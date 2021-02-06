package org.example.source;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.example.entity.Student;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
public class SourceDemo04_Customer_MySQL {

    public static void main(String[] args) throws Exception {

        // 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // source
        DataStream<Student> studentDs = env.addSource(new MySQLSource()).setParallelism(1);

        // sink
        studentDs.print();


        env.execute();


    }

    public static class MySQLSource extends RichParallelSourceFunction<Student>{
        private boolean flag = true;
        private Connection conn = null;
        private PreparedStatement ps = null;
        private ResultSet rs = null;

        // open 只执行一次，适合开启资源
        @Override
        public void open(Configuration parameters) throws Exception {
            conn = DriverManager.getConnection("jdbc:mysql://10.10.19.235:3306/example?useSSL=false","root","asd2828");
            String sql = "select id , name ,age from t_student ";
            ps = conn.prepareStatement(sql);
        }

        @Override
        public void run(SourceContext<Student> ctx) throws Exception {
            while (flag){
                rs = ps.executeQuery();
                while (rs.next()){
                    int id = rs.getInt("id");
                    String name = rs.getString("name");
                    int age = rs.getInt("age");

                    ctx.collect(new Student(id,name,age));
                }
                Thread.sleep(5000);
            }
        }

        // 接收到cancel 命令时，取消数据生成
        @Override
        public void cancel() {
            flag = false;
        }

        // close里面关闭资源

        @Override
        public void close() throws Exception {
            if (conn != null){
                conn.close();
            }
            if (ps != null){
                ps.close();
            }
            if (rs != null){
                rs.close();
            }
        }
    }


}
