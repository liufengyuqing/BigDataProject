package myproducer;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;

/**
 * create by liuzhiwei on 2020/3/28
 */
public class WriteToMySQL {
    public static void main(String[] args) throws ClassNotFoundException {
        String[] connection = {"T", "F"};
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://localhost:3306/collection_prod?characterEncoding=UTF-8&useSSL=false";
        String user = "root";
        String pass = "123456";
        Connection conn = null;
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, user, pass);
            String sql = "insert into collection_prod.pingan_call_detail (collector_id,call_direction,connected,RECORD_DURATION,time_dialing ) values(?,?,?,?,?)";
            PreparedStatement ps = conn.prepareStatement(sql);
            while (true) {
                Thread.sleep(3000);
                //int id = new Random().nextInt(10);
                int collector_id = new Random().nextInt(5);
                int call_direction = new Random().nextInt(2) + 1;
                String connected = connection[new Random().nextInt(connection.length)];
                int RECORD_DURATION = new Random().nextInt(200);
                long time_dialing = System.currentTimeMillis();
                System.out.println("collector_id: " + collector_id + " call_direction: " +
                        call_direction + " connected: " + connected + " RECORD_DURATION: " + RECORD_DURATION + " time_dialing: " + time_dialing);
                ps.setInt(1, collector_id);
                ps.setInt(2, call_direction);
                ps.setString(3, connected);
                ps.setInt(4, RECORD_DURATION);
                ps.setLong(5, time_dialing);
                ps.execute();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static PreparedStatement randomData(PreparedStatement ps) {
        String[] connection = {"T", "F"};
        int[] direction = new int[]{1, 2};
        for (int i = 0; i < 10; i++) {
            //int id = new Random().nextInt(10);
            int collector_id = new Random().nextInt(5);
            int index = new Random().nextInt(direction.length);
            System.out.println(index);
            int call_direction = direction[index];

            String connected = connection[new Random().nextInt(connection.length)];
            int RECORD_DURATION = new Random().nextInt(10);
            long time_dialing = System.currentTimeMillis();
            System.out.println("collector_id: " + collector_id + " call_direction: " +
                    call_direction + " connected: " + connected + " RECORD_DURATION: " + RECORD_DURATION + " time_dialing: " + time_dialing);
            try {
                ps.setInt(1, collector_id);

                ps.setInt(2, call_direction);
                ps.setString(3, connected);
                ps.setInt(4, RECORD_DURATION);
                ps.setLong(5, time_dialing);
                // ps.execute();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return ps;
    }

    public static Connection getConnection(String driver, String url, String user, String pass) {
        Connection conn = null;
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, user, pass);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;

    }
}
