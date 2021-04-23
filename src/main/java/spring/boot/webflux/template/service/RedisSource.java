package spring.boot.webflux.template.service;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@Slf4j
@Service
public class RedisSource {

    private RedisClusterAsyncCommands<String, String> pushFilterAsyncCommands;
    int sum = 0;

    public RedisSource(
            @Qualifier("push-filter-async") RedisClusterAsyncCommands<String, String> pushFilterAsyncCommands) {
        this.pushFilterAsyncCommands = pushFilterAsyncCommands;
    }

    private void incSum(int delta) {
        sum += delta;
    }

    public void loadFromRedis(List<Integer> buckets) {
        log.info("========= {}", buckets);
        try {
            log.info("time: {}", System.currentTimeMillis());
            //sync way
            /*
            for (Integer b : buckets) {
                List<String> res = pushFilterAsyncCommands.lrange(String.format("users-%d", b), 0, -1).get();
                log.info("chunck size: {}, sample: {}", res.size(), res.get(0).substring(0,20));
            }*/
            // async
            List<RedisFuture<List<String>>> futures = buckets
                    .stream()
                    .map(p -> String.format("users-%d", p))
                    .map(redisKey -> pushFilterAsyncCommands.lrange(redisKey, 0, -1))
                    .collect(Collectors.toList());
            pushFilterAsyncCommands.flushCommands();
            if (!LettuceFutures.awaitAll(Duration.ofMillis(5000), futures.toArray(new RedisFuture[futures.size()]))) {
                throw new TimeoutException("Redis command timeout. timeout: ");
            }
            log.info("done: {}", System.currentTimeMillis());
            futures.stream().forEach(f -> {
                List<String> v = null;
                try {
                    v = f.get();
                    incSum(v.size());
                    log.info("chunck size: {}, sample: {}", v.size(), v.get(0).substring(0,20));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            });
            log.info("sum of chunks: {}", this.sum);
        } catch (Exception e) {
            e.printStackTrace();
            log.error("clickhouse fetch failed : e : {}", e.getMessage());
        }
    }
}
