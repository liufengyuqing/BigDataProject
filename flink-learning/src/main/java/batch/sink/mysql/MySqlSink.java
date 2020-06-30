package batch.sink.mysql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;


/**
 * create by liuzhiwei on 2020/4/1
 * 数据批量 sink 数据到 mysql
 */
public class MySqlSink extends RichSinkFunction<Student> {
    private Connection connection = null;
    private PreparedStatement ps = null;

    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConnection();
        String sql = "insert into student values(?,?,?,?)";
        ps = connection.prepareStatement(sql);
    }

    /**
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
        //关闭连接和释放资源
        if (ps != null) {
            ps.close();
        }

        if (connection != null) {
            connection.close();
        }
    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     *
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(Student value, Context context) throws Exception {
        ps.setInt(1, value.getId());
        ps.setString(2, value.getName());
        ps.setInt(3, value.getAge());
        ps.setString(4, value.getSex());
        ps.execute();
    }

    private Connection getConnection() {
        Connection connection = null;
        String url = "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8";
        String user = "root";
        String pass = "123456";
        try {
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection(url, user, pass);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

}
