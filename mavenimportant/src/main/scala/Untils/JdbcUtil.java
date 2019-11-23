package Untils;

import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class JdbcUtil {

    private static DataSource dataSource = null;

    private static Properties prop = new Properties();

    private volatile static Connection conn = null;

    static {
        try {
            InputStream in = JdbcUtil.class.getClassLoader().getResourceAsStream("jdbcConfig.properties");
            prop.load(in);
            dataSource = DruidDataSourceFactory.createDataSource(prop);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Connection getConn(){
        if (conn==null){
            try {
                conn = dataSource.getConnection();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return conn;
    }


    public static void close(){
        if (conn==null){
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
