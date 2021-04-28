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
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.graalvm.compiler.graph.spi.Canonicalizable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.cache.CacheProperties;
import org.springframework.stereotype.Service;

import javax.cache.Cache;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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
                processTask(thickClient, parts[1]);
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

    private void processTask(Ignite client, String msg) {
        IgniteCompute compute = client.compute(client.cluster().forServers());
        long start = System.currentTimeMillis();
        String[] partitions = msg.split(",");
        List<IgniteFuture<List<BinaryObject>>> futures = Arrays.stream(partitions)
            .map(Integer::parseInt)
            .map(p -> compute.affinityCallAsync(CacheName, p, new ReadScanTask(p)))
            .collect(Collectors.toList());
        futures.stream().forEach((f) -> f.listen(ret -> {
            int counter = 0;
            for (BinaryObject obj : ret.get()) {
                counter++;
                if (counter % 10000 == 0) log.info("sample :{}", obj);
            }
            log.info("the result: {}", counter);
        }));
        log.info("quick return {}", System.currentTimeMillis() - start);
        /*
        for (String p: partitions) {
            long b = System.currentTimeMillis();
            int partition = Integer.parseInt(p);
            List<BinaryObject> res = compute.affinityCall(CacheName, partition, new ReadScanTask(partition));
            for (BinaryObject obj : res) {
                counter++;
                if (counter % 10000 == 0) log.info("sample :{}", obj);
            }
            log.info("agg : took{}", System.currentTimeMillis() -b);
        }*/
        //log.info("end of processing, cnt: {} ,took: {}", counter, System.currentTimeMillis() - start);
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
            long start = System.currentTimeMillis();
            IgniteBiPredicate<Long, BinaryObject> filter = (key, p) -> p.field("edition").equals("ja_JP");
            IgniteCache<Long, BinaryObject> cache = ignite.cache("POCUSER").withKeepBinary();

            List<BinaryObject> obs = new ArrayList<>();
            try (QueryCursor<Cache.Entry<Long, BinaryObject>> qryCursor = cache.query(new ScanQuery<>(filter).setPartition(partition).setLocal(true))) {
                qryCursor.forEach((c) -> obs.add(c.getValue()));
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println(String.format("finished : %d, took: %d", obs.size(), System.currentTimeMillis() - start));
            return obs;
        }

    }

    private static class ReadTask implements IgniteCallable<List<BinaryObject>> {
        private int partition;

        public ReadTask(int partition) {
            this.partition = partition;
        }

        @IgniteInstanceResource
        private Ignite ignite;

        @Override
        public List<BinaryObject> call() {
            System.out.println("get call with partition ids " + partition);
            long start = System.currentTimeMillis();
            IgniteCache<Long, BinaryObject> cache = ignite.cache("POCUSER").withKeepBinary();
            List<BinaryObject> res = new ArrayList<>();
            SqlFieldsQuery sql = new SqlFieldsQuery(
                  //  "select id, deviceToken, pushToken, generalConf, serverConf, profile, createTimestamp, updateTimestamp, code from pocuser.user where edition='ja_JP'")
                "select id, deviceToken, generalConf from pocuser.user where edition='ja_JP'"
            ).setPartitions(partition).setLocal(true);
            try {
                FieldsQueryCursor<List<?>> cursor = cache.query(sql);
                cursor.forEach((row)-> {
                    BinaryObjectBuilder builder = ignite.binary().builder("spring.boot.webflux.template.model");
                    builder.setField("id", row.get(0));
                    builder.setField("deviceToken", row.get(1));
                    //builder.setField("pushToken", row.get(2));
                    builder.setField("generalConf", row.get(3));
                    //builder.setField("serverConf", row.get(4));
                    //builder.setField("profile", row.get(5));
                    //builder.setField("createTimestamp", row.get(6));
                    //builder.setField("updateTimestamp", row.get(7));
                    //builder.setField("code", row.get(8));
                    res.add(builder.build());
                });
                cursor.close();
            /*
                List<List<?>> ret = cache.query(sql).getAll();
                for (List<?> row : ret) {
                    BinaryObjectBuilder builder = ignite.binary().builder("spring.boot.webflux.template.model");
                    builder.setField("id", row.get(0));
                    builder.setField("deviceToken", row.get(1));
                    builder.setField("pushToken", row.get(2));
                    builder.setField("generalConf", row.get(3));
                    builder.setField("serverConf", row.get(4));
                    builder.setField("profile", row.get(5));
                    builder.setField("createTimestamp", row.get(6));
                    builder.setField("updateTimestamp", row.get(7));
                    builder.setField("code", row.get(8));
                    res.add(builder.build());
                }*/
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println(String.format("finished : %d, took: %d", res.size(), System.currentTimeMillis() - start));
            return res;
        }
    }
}
