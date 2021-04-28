package spring.boot.webflux.template.model;

import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class User {
    @QuerySqlField(index = true)
    public long id;

    @QuerySqlField(index = true)
    public String deviceToken;

    @QuerySqlField
    public String code;

    @QuerySqlField
    public String pushToken;

    @QuerySqlField(index = true)
    public String edition;

    @QuerySqlField
    public long createTimestamp;

    @QuerySqlField
    public long updateTimestamp;

    @QuerySqlField
    public String generalConf;

    @QuerySqlField
    public String serverConf;

    @QuerySqlField
    public String profile;

    @QuerySqlField(index = true)
    public int morning;


}
