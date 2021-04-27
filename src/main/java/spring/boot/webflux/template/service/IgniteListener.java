package spring.boot.webflux.template.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.cache.CacheProperties;
import org.springframework.stereotype.Service;

import javax.cache.Cache;
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
            String[] parts = ((String)msg).split(":");
            if (parts[0].equals("redis")) {
                processRedisTask(parts[1]);
            } else if (parts[0].equals("sql")) {
                processSQLTask(parts[1]);
            } else if (parts[0].equals("task")) {
                //processScanTask(thickClient.compute(thickClient.cluster().forServers()), parts[1]);
                processTask(thickClient.compute(thickClient.cluster().forServers()), parts[1]);
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
        //dbSource.loadFromDB(offset, limit);
        //prestoSource.loadFromPresto(offset, limit);
        prestoSource.loadUsersFromPresto(offset, limit);
    }

    private void processTask(IgniteCompute compute, String msg) {
        long start = System.currentTimeMillis();
        String[] partitions = msg.split(",");
        int counter = 0;
        for (String p: partitions) {
            int partition = Integer.parseInt(p);
            List<String> res = compute.affinityCall(CacheName, partition, new ReadTask(partition));
            for (String obj : res) {
                counter++;
                if (counter % 1000 == 0) log.info("sample :{}", obj);
            }
        }
        log.info("end of processing, cnt: {} ,took: {}", counter, System.currentTimeMillis() - start);
    }

    private void processScanTask(IgniteCompute compute, String msg) {
        long start = System.currentTimeMillis();
        String[] partitions = msg.split(",");
        int counter = 0;
        for (String p: partitions) {
            int partition = Integer.parseInt(p);
            List<BinaryObject> res = compute.affinityCall(CacheName, partition, new ReadScanTask(partition));
            for (BinaryObject obj : res) {
                counter++;
                if (counter % 1000 == 0) log.info("sample :{}", obj);
            }
        }
        log.info("end of processing, cnt: {} ,took: {}", counter, System.currentTimeMillis() - start);
    }

    private static class ReadScanTask implements IgniteCallable<List<BinaryObject>> {
        private int partition;

        public ReadScanTask(int partition) {
            this.partition = partition;
        }

        @IgniteInstanceResource
        private Ignite ignite;

        @Override
        public List<BinaryObject> call() {
            System.out.println("get call with partition id >>>>>>>>>>>>>>              " + partition);
            IgniteBiPredicate<Long, BinaryObject> filter = (key, p) -> p.field("edition").equals("ja_JP");
            IgniteCache<Long, BinaryObject> cache = ignite.cache("POCUSER").withKeepBinary();

            List<BinaryObject> obs = new ArrayList<>();
            try (QueryCursor<Cache.Entry<Long, BinaryObject>> qryCursor = cache.query(new ScanQuery<>(filter).setPartition(partition))) {
                qryCursor.forEach((c) -> obs.add(c.getValue()));
            } catch (Exception e) {
                e.printStackTrace();
            }
            return obs;
        }
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
                    "select * from pocuser.user where edition='ja_JP'").setPartitions(partition);
            try {
                List<List<?>> ret = cache.query(sql).getAll();
                for (List<?> row : ret) {
                    //String r = String.format("%d, %s, %s, %s, %d", row.get(0), row.get(1), row.get(2), row.get(3), row.get(4));
                    String r = row.toString();
                    res.add(r);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return res;
        }
    }
}
