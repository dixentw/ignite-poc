package spring.boot.webflux.template.service;

import com.github.housepower.jdbc.BalancedClickhouseDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.IgniteDataStreamer;
import org.springframework.stereotype.Service;
import spring.boot.webflux.template.model.User;

import java.sql.*;
import java.util.*;

@Slf4j
@Service
public class PrestoSource {
    private final String dbName = "hive.default";
    private final String tableName = "users";


    public void clickHouseBatchInsert(List<Map<String, Object>> users) {
        try {
            long startTime = System.currentTimeMillis();
            String clickhouseIPAddr = "10.0.15.17:9000,10.0.15.16:9000,10.0.15.15:9000,10.0.15.14:9000";
            String jdbcUrl = String.format("jdbc:clickhouse://%s?user=default&password=smartnews&database=%s", clickhouseIPAddr, "testdb");
            BalancedClickhouseDataSource clickHouseDataSource = new BalancedClickhouseDataSource(jdbcUrl);
            log.info("jdbc url : {}", jdbcUrl);
            Connection conn = clickHouseDataSource.getConnection();
            conn.setAutoCommit(false);
            PreparedStatement pstmt = conn.prepareStatement(
                    "INSERT INTO push_user_3_all (id, device_token, code, push_token, creation_timestamp, update_timestamp, general_configuration, server_configuration" +
                            "profile, last_delivery_timestamp, last_survey_timestamp, edition, push_time, sign)" +
                            " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
            int cnt = 0;
            for (Map<String, Object> u: users) {
                String edition = "";
                if(((Long) u.get("id")) % 2 ==0) {
                    edition = "ja_JP";
                } else {
                    edition = "en_US";
                }
                pstmt.setLong(1, (Long) u.get("id"));
                pstmt.setString(2, (String) u.get("device_token"));
                pstmt.setString(3, (String) u.get("code"));
                pstmt.setString(4, (String) u.get("push_token"));
                pstmt.setLong(5, (Long) u.get("create_timestamp"));
                pstmt.setLong(6, (Long) u.get("update_timestamp"));
                pstmt.setString(7, (String) u.get("general_configuration"));
                pstmt.setString(8, (String) u.get("server_configuration"));
                pstmt.setString(9, (String) u.get("profile"));
                pstmt.setLong(10, (Long) u.get("create_timestamp"));
                pstmt.setLong(11, (Long) u.get("create_timestamp"));
                pstmt.setString(12, edition);
                pstmt.setString(13, "10800");
                pstmt.setInt(14, 10);
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
            String sql = String.format("select id, code, create_timestamp, update_timestamp device_token, push_token, general_configuration, server_configuration, profile from %s.%s where id >%d and id<=%d and dt='2021-04-26'", dbName, tableName, offset, offset+length);
            log.info("try to query as =====   {}", sql);
            ResultSet rs = statement.executeQuery(sql);
            int cnt = 0;
            List<Map<String, Object>> users = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> map = new HashMap<>();
                map.put("id", rs.getLong("id"));
                map.put("code", rs.getString("code"));
                map.put("create_timestamp", rs.getLong("create_timestamp"));
                map.put("update_timestamp", rs.getLong("update_timestamp"));
                map.put("device_token", rs.getString("device_token"));
                map.put("push_token", rs.getString("push_token"));
                map.put("general_configuration", rs.getString("general_configuration"));
                map.put("server_configuration", rs.getString("server_configuration"));
                map.put("profile", rs.getString("profile"));
                cnt++;
                users.add(map);
                if (cnt % 10000 ==0) {
                    log.info("sample user: {}", map);
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
