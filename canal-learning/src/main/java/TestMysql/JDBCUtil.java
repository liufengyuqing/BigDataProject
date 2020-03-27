package TestMysql;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

/**
 * create by liuzhiwei on 2020/3/23
 */
public class JDBCUtil {
    public static Connection getConnection() throws IOException {

        Connection connection = null;
        /**
         * 不建议硬编码到代码中
         */
//        String url = "jdbc:mysql://localhost:3306/test?characterEncoding=utf-8&useSSL=true&serverTimeZone=GMT";
//        String user = "root";
//        String password = "123456";
//        String driverClass = "com.mysql.cj.jdbc.Driver";


        final InputStream resourceAsStream = JDBCUtil.class.getClassLoader().getResourceAsStream("db.properties");
        Properties properties = new Properties();
        properties.load(resourceAsStream);

        String url = properties.getProperty("jdbc.url");
        String user = properties.getProperty("jdbc.username");
        String password = properties.getProperty("jdbc.password");
        String driverClass = properties.getProperty("jdbc.driverClassName");

        try {
            Class.forName(driverClass);
            connection = DriverManager.getConnection(url, user, password);


        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    public static void release(ResultSet resultSet, Statement statement, Connection connection) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
