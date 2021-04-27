package spring.boot.webflux.template.model;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class User {
    @QuerySqlField(index = true)
    public long id;

    @QuerySqlField(index = true)
    public String deviceToken;

    public String pushToken;

    public String setting;

    @QuerySqlField(index = true)
    public String edition;

    @QuerySqlField
    public String platform;

    @QuerySqlField(index = true)
    public int morning;

    @QuerySqlField
    public int aid;

    @Override
    public String toString() {
        return String.format("%d, %s, %s, %s, %d", id, deviceToken, pushToken, setting, aid);
    }
}
