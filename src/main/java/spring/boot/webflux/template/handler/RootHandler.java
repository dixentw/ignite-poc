package spring.boot.webflux.template.handler;

import com.google.common.collect.ImmutableMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;
import spring.boot.webflux.template.service.IgniteService;

import java.time.LocalDateTime;

@Component
public class RootHandler {
    @Value("${application.name}")
    private String appName;
    private final IgniteService iService;

    @Autowired
    public RootHandler(IgniteService iService) {
        this.iService = iService;
    }

    public Mono<ServerResponse> root(ServerRequest request) {
        ImmutableMap<Object, Object> entity = ImmutableMap.builder()
                .put("name", appName)
                .put("java.version", System.getProperty("java.version"))
                .put("now", LocalDateTime.now())
                .build();
        return ServerResponse.ok().contentType(MediaType.APPLICATION_JSON)
                .body(BodyInserters.fromValue(entity));
    }

    public Mono<ServerResponse> load(ServerRequest request) {
        try {
            iService.loadData();
        } catch (Exception e) {
            e.printStackTrace();
            return ServerResponse.badRequest().contentType(MediaType.APPLICATION_JSON).body(BodyInserters.empty());
        }
        ImmutableMap<Object, Object> entity = ImmutableMap.builder()
                .put("name", appName)
                .put("java.version", System.getProperty("java.version"))
                .put("now", LocalDateTime.now())
                .build();
        return ServerResponse.ok().contentType(MediaType.APPLICATION_JSON)
                .body(BodyInserters.fromValue(entity));
    }

    public Mono<ServerResponse> loadTradition(ServerRequest request) {
        try {
            iService.loadDataTradition();
        } catch (Exception e) {
            e.printStackTrace();
            return ServerResponse.badRequest().contentType(MediaType.APPLICATION_JSON).body(BodyInserters.empty());
        }
        ImmutableMap<Object, Object> entity = ImmutableMap.builder()
                .put("name", appName)
                .put("java.version", System.getProperty("java.version"))
                .put("now", LocalDateTime.now())
                .build();
        return ServerResponse.ok().contentType(MediaType.APPLICATION_JSON)
                .body(BodyInserters.fromValue(entity));
    }

    public Mono<ServerResponse> run(ServerRequest request) {
        try {
            iService.runTask();
        } catch (Exception e) {
            e.printStackTrace();
            return ServerResponse.badRequest().contentType(MediaType.APPLICATION_JSON).body(BodyInserters.empty());
        }
        ImmutableMap<Object, Object> entity = ImmutableMap.builder()
                .put("name", appName)
                .put("java.version", System.getProperty("java.version"))
                .put("now", LocalDateTime.now())
                .build();
        return ServerResponse.ok().contentType(MediaType.APPLICATION_JSON)
                .body(BodyInserters.fromValue(entity));
    }

    public Mono<ServerResponse> runSQL(ServerRequest request) {
        try {
            iService.runSQLTask();
        } catch (Exception e) {
            e.printStackTrace();
            return ServerResponse.badRequest().contentType(MediaType.APPLICATION_JSON).body(BodyInserters.empty());
        }
        return ServerResponse.ok().contentType(MediaType.APPLICATION_JSON)
                .body(BodyInserters.fromValue("done"));
    }
    public Mono<ServerResponse> runRedis(ServerRequest request) {
        try {
            iService.runRedisTask();
        } catch (Exception e) {
            e.printStackTrace();
            return ServerResponse.badRequest().contentType(MediaType.APPLICATION_JSON).body(BodyInserters.empty());
        }
        return ServerResponse.ok().contentType(MediaType.APPLICATION_JSON)
                .body(BodyInserters.fromValue("done"));
    }

}
