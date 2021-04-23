package spring.boot.webflux.template.service;

import com.opencsv.CSVReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import spring.boot.webflux.template.model.User;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

@Service
@Slf4j
public class IgniteService {

    private Ignite thickClient;
    //private S3Client s3Client;
//    private SqsAsyncClient sqsClient;

    @Autowired
    public IgniteService(Ignite thickClient) {
        this.thickClient = thickClient;
//        this.s3Client = s3Client;
//        this.sqsClient = sqsClient;
    }

    public void runTask() {
        Affinity<Long> affinityFunc = thickClient.affinity("POCUSER");
        List<Integer> parts  = new ArrayList<>();
        Random random = new Random();
        for (ClusterNode n : thickClient.cluster().forServers().nodes()) {
            int[] ps = affinityFunc.primaryPartitions(n);
            for (int p : ps) parts.add(p);
        }
        log.info("list partitions: {}", parts);
        // job distribution by hand
        Map<String, List<String>> tasks = new HashMap<>();
        List<String> ids = thickClient.cluster().forClients().nodes()
            .stream()
            .map(n -> n.id().toString())
            .collect(Collectors.toList());
        for (int p : parts) {
            String node = ids.get(random.nextInt(ids.size()));
            List<String> ps = tasks.getOrDefault(node, new ArrayList<>());
            ps.add(String.valueOf(p));
            tasks.put(node, ps);
        }
        log.info("tasks distribution : {}", tasks);
        for (Map.Entry<String, List<String>> entry : tasks.entrySet()) {
            String joined = String.join(",", entry.getValue());
            log.info("going to send node: {}, tasks: {}", entry.getKey(), joined);
            thickClient.message().send(entry.getKey(), joined);
        }
    }

    public void runSQLTask() {
        List<String> ids = thickClient.cluster().nodes()
                .stream()
                .map(n -> n.id().toString())
                .collect(Collectors.toList());
        int length = 560000;
        int cnt = 0;
        for (String id : ids) {
            String msg = String.format("sql:%d,%d", cnt*length, length);
            cnt++;
            log.info("going to send node: {}, task: {}", id, msg);
            thickClient.message().send(id, msg);
        }
    }

    public void runRedisTask() {
        int userbuckets = 10000;
        List<String> ids = thickClient.cluster().nodes()
                .stream()
                .map(n -> n.id().toString())
                .collect(Collectors.toList());
        // round robin
        Map<String, List<String>> tasks = new HashMap<>();
        for (int i=0; i<userbuckets; i++) {
            int mod = i % ids.size();
            List<String> task = tasks.getOrDefault(ids.get(mod), new ArrayList<>());
            task.add(String.valueOf(i));
            tasks.put(ids.get(mod), task);
        }
        for (String id : ids) {
            String msg = String.join(",", tasks.get(id));
            log.info("going to send node: {}, task: {}", id, "redis:"+msg);
            thickClient.message().send(id, msg);
        }
    }

    public void loadData() throws IOException {
        Reader reader = Files.newBufferedReader(
                FileSystems.getDefault().getPath("/tmp", "lala.dixen.csv"));
        CSVReader csvReader = new CSVReader(reader);
        String[] line;
        CacheConfiguration<Long, User> personCacheCfg = new CacheConfiguration<>();
        personCacheCfg.setName("POCUSER");
        personCacheCfg.setIndexedTypes(Long.class, User.class);
        thickClient.getOrCreateCache(personCacheCfg);
        IgniteDataStreamer<Long, User> stmr = thickClient.dataStreamer("POCUSER");
        stmr.allowOverwrite(true);
        log.info("start loading...");
        int counter = 0;
        while ((line = csvReader.readNext()) != null) {
            User u = new User();
            u.id = Long.parseLong(line[0]);
            u.deviceToken = line[1];
            u.pushToken = line[3];
            u.setting = line[6];
            u.edition = line[12];
            u.platform = line[11];
            u.aid = (int) (u.id % 8);
            stmr.addData(u.id, u);
            counter++;
        }
        stmr.flush();
        stmr.close();
        log.info("end of loading..., with count: {}", counter);
        reader.close();
        csvReader.close();
    }

    public void loadDataTradition() throws IOException {
        /*
        S3Object o = s3Client.getObject("smartnews-log-dev", "lala.dixen.csv.gz");
        S3ObjectInputStream s3is = o.getObjectContent();
        GZIPInputStream gis = new GZIPInputStream(s3is);
        BufferedReader reader = new BufferedReader(new InputStreamReader(gis));
        CSVReader csvReader = new CSVReader(reader);
        // re-create cache
        CacheConfiguration<Long, User> personCacheCfg = new CacheConfiguration<>();
        personCacheCfg.setName("POCUSER");
        personCacheCfg.setIndexedTypes(Long.class, User.class);
        thickClient.getOrCreateCache(personCacheCfg);
        IgniteDataStreamer<Long, User> stmr = thickClient.dataStreamer("POCUSER");
        stmr.allowOverwrite(true);
        log.info("start loading...");
        int counter = 0;
        String[] line;
        while ((line = csvReader.readNext()) != null) {
            User u = new User();
            u.id = Long.parseLong(line[0]);
            u.deviceToken = line[1];
            u.pushToken = line[3];
            u.setting = line[6];
            u.aid = (int) (u.id % 8);
            stmr.addData(u.id, u);
            counter++;
        }
        stmr.flush();
        stmr.close();
        reader.close();
        csvReader.close();
        log.info("end of loading..., with count: {}", counter);
         */
    }

}
