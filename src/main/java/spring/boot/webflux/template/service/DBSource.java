package spring.boot.webflux.template.service;

import com.github.housepower.jdbc.BalancedClickhouseDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

@Slf4j
@Service
public class DBSource {
    private final String dbName = "testdb";
    private final String tableName = "push_user_4_all";

    public void loadFromDB(int offset, int length) {
        log.info("parameters, off: {}, length {}", offset, length);
        try {
            long startTime = System.currentTimeMillis();
            String clickhouseIPAddr = "10.0.15.17:9000,10.0.15.16:9000,10.0.15.15:9000,10.0.15.14:9000";
            String jdbcUrl = String.format("jdbc:clickhouse://%s?user=default&password=smartnews&database=%s", clickhouseIPAddr, "testdb");
            BalancedClickhouseDataSource clickHouseDataSource = new BalancedClickhouseDataSource(jdbcUrl);
            log.info("jdbc url : {}", jdbcUrl);
            Connection connection = clickHouseDataSource.getConnection();
            Statement statement = connection.createStatement();
            String sql = String.format("select * from %s.%s where edition='ja_JP' and id > %d and id <= %d", dbName, tableName, offset, offset+length);
            log.info("try to query as =====   {}", sql);
            ResultSet rs = statement.executeQuery(sql);
            int cnt = 0;
            while (rs.next()) {
                cnt++;
                if (cnt % 10000 ==0) log.info("sample user: {}", rs.getString("device_token"));
            }
            long endTime = System.currentTimeMillis();
            log.info("end of query clickhouse cnt : {}, time lasp: {}", cnt, endTime-startTime);
        } catch (Exception e) {
            e.printStackTrace();
            log.error("clickhouse fetch failed : e : {}", e.getMessage());
        }
    }
}
