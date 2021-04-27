package spring.boot.webflux.template.service;

import com.github.housepower.jdbc.BalancedClickhouseDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.IgniteDataStreamer;
import org.springframework.stereotype.Service;
import spring.boot.webflux.template.model.User;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Slf4j
@Service
public class PrestoSource {
    private final String dbName = "hive.default";
    private final String tableName = "users";


    public void clickHouseBatchInsert(List<User> users) {
        try {
            long startTime = System.currentTimeMillis();
            String clickhouseIPAddr = "10.0.15.17:9000,10.0.15.16:9000,10.0.15.15:9000,10.0.15.14:9000";
            String jdbcUrl = String.format("jdbc:clickhouse://%s?user=default&password=smartnews&database=%s", clickhouseIPAddr, "testdb");
            BalancedClickhouseDataSource clickHouseDataSource = new BalancedClickhouseDataSource(jdbcUrl);
            log.info("jdbc url : {}", jdbcUrl);
            Connection conn = clickHouseDataSource.getConnection();
            conn.setAutoCommit(false);
            PreparedStatement pstmt = conn.prepareStatement(
                    "INSERT INTO push_user_2 (id, Last_Name, " +
                            "First_Name, Email, Phone_Number)" +
                            " VALUES(?,?,?,?,?)");
            int cnt = 0;
            for (User u: users) {
                pstmt.setLong(1, u.id);
                pstmt.setString(2, u.deviceToken);
                pstmt.addBatch();
                cnt++;
            }
            try {
                pstmt.executeBatch();
            } catch (SQLException e) {
                log.info("error :{}", e.getMessage());
            }
            conn.commit();
            long endTime = System.currentTimeMillis();
            log.info("end of query clickhouse cnt : {}, time lasp: {}", cnt, endTime-startTime);
        } catch (Exception e) {
            e.printStackTrace();
            log.error("clickhouse fetch failed : e : {}", e.getMessage());
        }
    }


    public void loadUsersFromPresto(int offset, int length) {
        log.info("parameters, off: {}, length {}", offset, length);
        offset += 100;
        try {
            // URL parameters
            String url = "jdbc:presto://presto.smartnews.internal:8081/hive/smartnews";
            Properties properties = new Properties();
            properties.setProperty("user", "nobody");
            Connection connection = DriverManager.getConnection(url, properties);
            Statement statement = connection.createStatement();
            String sql = String.format("select id, device_token, push_token, general_configuration from %s.%s where id >%d and id<=%d and dt='2021-04-26'", dbName, tableName, offset, offset+length);
            log.info("try to query as =====   {}", sql);
            ResultSet rs = statement.executeQuery(sql);
            int cnt = 0;
            List<User> users = new ArrayList<>();
            while (rs.next()) {
                User u = new User();
                u.id = rs.getLong("id");
                u.setting = rs.getString("general_configuration");
                u.deviceToken = rs.getString("device_token");
                u.pushToken = rs.getString("push_token");
                if (u.id % 2 == 0) {
                    u.edition = "ja_JP";
                } else {
                    u.edition = "en_US";
                }
                if (u.id % 3 == 0) {
                    u.morning = 10800;
                } else if (u.id % 3 == 1) {
                    u.morning = 97000;
                } else if (u.id % 3 == 2) {
                    u.morning = 12600;
                }
                cnt++;
                users.add(u);
                if (cnt % 10000 ==0) {
                    log.info("sample user: {}", u);
                    clickHouseBatchInsert(users);
                    users.clear();
                }
            }
            clickHouseBatchInsert(users);
            log.info("end of loading..., with count: {}", cnt);
        } catch (Exception e) {
            e.printStackTrace();
            log.error("presto fetch failed : e : {}", e.getMessage());
        }
    }


    public void loadFromPresto(int offset, int length) {
        log.info("parameters, off: {}, length {}", offset, length);
        offset += 100;
        try {
            long startTime = System.currentTimeMillis();
            // URL parameters
            String url = "jdbc:presto://presto.smartnews.internal:8081/hive/smartnews";
            Properties properties = new Properties();
            properties.setProperty("user", "nobody");
            Connection connection = DriverManager.getConnection(url, properties);
            Statement statement = connection.createStatement();
            String sql = String.format("select id, device_token, push_token from %s.%s where id >%d and id<=%d and dt='2021-04-21'", dbName, tableName, offset, offset+length);
            log.info("try to query as =====   {}", sql);
            ResultSet rs = statement.executeQuery(sql);
            int cnt = 0;
            while (rs.next()) {
                cnt++;
                if (cnt % 10000 ==0) log.info("sample user: {}", rs.getString("device_token"));
            }
            long endTime = System.currentTimeMillis();
            log.info("end of query presto cnt : {}, time lasp: {}", cnt, endTime-startTime);
        } catch (Exception e) {
            e.printStackTrace();
            log.error("presto fetch failed : e : {}", e.getMessage());
        }
    }
}
