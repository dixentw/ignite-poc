package spring.boot.webflux.template.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;

//@Configuration
@Slf4j
public class AWSConfig {
    @Bean
    public SqsAsyncClient sqsClient() {
        SqsAsyncClient client =
            SqsAsyncClient.builder().region(Region.AP_NORTHEAST_1).build();
        try {
            log.info("queue url : {}", client.getQueueUrl(GetQueueUrlRequest.builder().queueName("sn_push_canary_dev").build()).get());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return client;
    }
    @Bean
    public S3Client s3Client() {
        return S3Client.builder().region(Region.AP_NORTHEAST_1).build();
    }

}
