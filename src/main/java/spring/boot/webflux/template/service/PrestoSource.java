package spring.boot.webflux.template.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import spring.boot.webflux.template.model.User;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

@Slf4j
@Service
public class PrestoSource {
    private final String dbName = "hive.default";
    private final String tableName = "users";

    private Ignite thickClient;

    @Autowired
    public PrestoSource(Ignite client) {
        this.thickClient = client;
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
            String sql = String.format("select id, device_token, push_token, general_configuration from %s.%s where id >%d and id<=%d and dt='2021-04-25'", dbName, tableName, offset, offset+length);
            log.info("try to query as =====   {}", sql);
            ResultSet rs = statement.executeQuery(sql);
            IgniteDataStreamer<Long, User> stmr = thickClient.dataStreamer("POCUSER");
            int cnt = 0;
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
                if (cnt % 10000 ==0) log.info("sample user: {}", u);
                stmr.addData(u.id, u);
            }
            stmr.flush();
            stmr.close();
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
