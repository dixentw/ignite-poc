package spring.boot.webflux.template.config;

import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import io.lettuce.core.resource.DirContextDnsResolver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Configuration
@Slf4j
public class RedisConfig {

    @Bean(name = "push-filter-redis-client")
    public RedisClusterClient pushFilterRedisClient() {
        // keep seed node to one now
        RedisURI redisURI = RedisURI.builder().withHost("sn-push-filter-cluster-dev.bpuzex.clustercfg.apne1.cache.amazonaws.com").withPort(6379).build();
        RedisClusterClient redisClusterClient = RedisClusterClient.create(redisURI);
        SocketOptions socketOptions = SocketOptions.builder().keepAlive(true).build();

        ClusterTopologyRefreshOptions topologyRefreshOptions = ClusterTopologyRefreshOptions.builder()
                .enablePeriodicRefresh(Duration.ofMinutes(10))
                .enableAllAdaptiveRefreshTriggers()
                .build();

        redisClusterClient.setOptions(ClusterClientOptions.builder()
                .autoReconnect(true)
                .topologyRefreshOptions(topologyRefreshOptions)
                .socketOptions(socketOptions)
                .validateClusterNodeMembership(false)
                .build());

        redisClusterClient.refreshPartitions();
        return redisClusterClient;
    }

    @Bean(name = "push-filter-redis-connection")
    public StatefulRedisClusterConnection<String, String> pushFilterRedisConnection(@Qualifier("push-filter-redis-client") RedisClusterClient redisClusterClient) {
        StatefulRedisClusterConnection<String, String> stringStringStatefulRedisClusterConnection = redisClusterClient.connect();
        stringStringStatefulRedisClusterConnection.setReadFrom(ReadFrom.REPLICA_PREFERRED);
        return stringStringStatefulRedisClusterConnection;
    }


    @Bean(name = "push-filter-async")
    RedisClusterAsyncCommands<String, String> pushFilterAsyncCommands(
        @Qualifier("push-filter-redis-connection") StatefulRedisClusterConnection<String, String> redisConnection) {
        RedisClusterAsyncCommands<String, String> commands = redisConnection.async();
        commands.setTimeout(Duration.ofMillis(3000));
        commands.setAutoFlushCommands(true);
        return commands;
    }

}
