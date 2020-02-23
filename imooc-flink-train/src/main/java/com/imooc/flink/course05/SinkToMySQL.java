package com.imooc.flink.course05;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @ClassName: SinkToMySQL
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/2/9 12:14 下午
 */

public class SinkToMySQL extends RichSinkFunction<Student> {
    Connection connection;
    PreparedStatement pstmt;

    private Connection getConnection() {
        Connection conn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            String url = "jdbc:mysql://localhost:3306/imooc_flink";
            conn = DriverManager.getConnection(url, "root", "123456");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        connection = getConnection();
        String sql = "insert into student(id,name,age) values(?,?,?)";
        pstmt = connection.prepareStatement(sql);
    }

    //每条记录执行一次
    @Override
    public void invoke(Student value, Context context) throws Exception {
        System.out.println("invoke~~~~~~~~~");

        // 未前面的占位符赋值
        pstmt.setInt(1, value.getId());
        pstmt.setString(2, value.getName());
        pstmt.setInt(3, value.getAge());

        pstmt.executeUpdate();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (pstmt != null) {
            pstmt.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

}
