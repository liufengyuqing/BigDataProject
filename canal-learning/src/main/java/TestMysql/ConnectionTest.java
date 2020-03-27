package TestMysql;

import org.junit.Assert;

import java.io.IOException;
import java.sql.Connection;

/**
 * create by liuzhiwei on 2020/3/23
 */
public class ConnectionTest {
    public static void main(String[] args) {
        try {
            Connection connection = JDBCUtil.getConnection();
            System.out.println(connection.toString());
            Assert.assertNotNull(connection);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
