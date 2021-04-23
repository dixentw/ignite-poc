package spring.boot.webflux.template;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.config.EnableWebFlux;
import org.springframework.web.reactive.config.WebFluxConfigurer;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;
import spring.boot.webflux.template.handler.FileUploadHandler;
import spring.boot.webflux.template.handler.KeyValueStoreHandler;
import spring.boot.webflux.template.handler.RootHandler;

import static org.springframework.web.reactive.function.server.RequestPredicates.accept;

@Configuration
@EnableWebFlux
public class RouterConfig implements WebFluxConfigurer {

    @Bean
    public RouterFunction<ServerResponse> route(RootHandler rootHandler) {
        return RouterFunctions.route()
                .GET("/", accept(MediaType.ALL), rootHandler::root)
                .GET("/load", accept(MediaType.ALL), rootHandler::load)
                .GET("/loadt", accept(MediaType.ALL), rootHandler::loadTradition)
                .GET("/run", accept(MediaType.ALL), rootHandler::run)
                .GET("/runsql", accept(MediaType.ALL), rootHandler::runSQL)
                .GET("/runredis", accept(MediaType.ALL), rootHandler::runRedis)
        .build();
        }

}