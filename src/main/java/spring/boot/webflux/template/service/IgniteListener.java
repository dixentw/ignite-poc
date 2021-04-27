package spring.boot.webflux.template.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.cache.CacheProperties;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
@Slf4j
public class IgniteListener {
    private final int JSON_MAX_SIZE = 250_000;
    private final String queueName = "sn_push_canary_dev";
    private final String CacheName = "POCUSER";
    private DBSource dbSource;
    private RedisSource redisSource;
    private PrestoSource prestoSource;

    @Autowired
    public IgniteListener(Ignite thickClient, DBSource db, RedisSource redis, PrestoSource prestoSource) {
        IgniteMessaging messaging = thickClient.message();
        String topicName = thickClient.cluster().forLocal().node().id().toString();
        this.dbSource = db;
        this.redisSource = redis;
        this.prestoSource = prestoSource;
        log.info("listen to this topic: {}", topicName);
        messaging.localListen(topicName, (nodeId, msg) -> {
            log.info("get message: {}, the time: {}", msg, System.currentTimeMillis());
            //processTask(compute, (String)msg);
            String[] parts = ((String)msg).split(":");
            if (parts[0].equals("redis")) {
                processRedisTask(parts[1]);
            } else if (parts[0].equals("sql")) {
                processSQLTask(parts[1]);
            }
            log.info("done, {}", System.currentTimeMillis());
            return true;
        });
    }

    private void processRedisTask(String msg) {
        List<Integer> bs = Stream.of(msg.split(",")).map(Integer::parseInt).collect(Collectors.toList());
        this.redisSource.loadFromRedis(bs);
    }

    private void processSQLTask(String msg) {
        String[] partitions = msg.split(",");
        int offset = Integer.parseInt(partitions[0]);
        int limit = Integer.parseInt(partitions[1]);
        dbSource.loadFromDB(offset, limit);
        //prestoSource.loadUsersFromPresto(offset, limit);
    }

    private void processTask(IgniteCompute compute, String msg) {
        log.info("get message: {}, the time: {}", msg, System.currentTimeMillis());
        String[] partitions = msg.split(",");
        for (String p: partitions) {
            int partition = Integer.parseInt(p);
            List<String> res = compute.affinityCall(CacheName, partition, new ReadTask(partition));
            log.info("result size : {}", res.size());
            int counter = 0;
            List<String> sampleTokens = new ArrayList<>();
            for (String obj : res) {
                counter++;
                if (counter % 100 == 0) sampleTokens.add(obj);
            }
            log.info("sample :{}" + sampleTokens);
        }
        log.info("end of processing: {}", System.currentTimeMillis());
    }

    private static class ReadTask implements IgniteCallable<List<String>> {
        private int partition;

        public ReadTask(int partition) {
            this.partition = partition;
        }

        @IgniteInstanceResource
        private Ignite ignite;

        @Override
        public List<String> call() {
            System.out.println("get call with partition id >>>>>>>>>>>>>>              " + partition);
            IgniteCache<Long, BinaryObject> cache = ignite.cache("POCUSER").withKeepBinary();
            List<String> res = new ArrayList<>();
            SqlFieldsQuery sql = new SqlFieldsQuery(
                    "select * from pocuser.user where edition='en_US'").setPartitions(partition).setLocal(true);
            List<List<?>> ret = cache.query(sql).getAll();
            for (List<?> row : ret) {
                String r = String.format("%d, %s, %s, %s, %d", row.get(0), row.get(1), row.get(2), row.get(3), row.get(4));
                res.add(r);
            }
            return res;
        }
    }
}
