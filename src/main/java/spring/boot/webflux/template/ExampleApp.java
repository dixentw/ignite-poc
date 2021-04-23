package spring.boot.webflux.template;

import org.jetbrains.annotations.Async;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;


@SpringBootApplication
@EnableScheduling
public class ExampleApp {

    public static void main(String args[]) {
        SpringApplication.run(ExampleApp.class, args);
    }

}

