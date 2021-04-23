package spring.boot.webflux.template.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

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
