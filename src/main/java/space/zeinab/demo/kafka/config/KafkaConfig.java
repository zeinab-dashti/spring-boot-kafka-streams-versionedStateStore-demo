package space.zeinab.demo.kafka.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Slf4j
@Configuration
public class KafkaConfig {
    public static final String CUSTOMER_VERSIONED_TOPIC = "customers-versioned-topic";
    public static final String CUSTOMER_UNVERSIONED_TOPIC = "customers-unversioned-topic";
    public static final String ORDER_VERSIONED_TOPIC = "orders-versioned-topic";
    public static final String ORDER_UNVERSIONED_TOPIC = "orders-unversioned-topic";

    @Bean
    public NewTopic customerVersionedTopic() {
        return TopicBuilder.name(CUSTOMER_VERSIONED_TOPIC)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic customerUnversionedTopic() {
        return TopicBuilder.name(CUSTOMER_UNVERSIONED_TOPIC)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic orderVersionedTopic() {
        return TopicBuilder.name(ORDER_VERSIONED_TOPIC)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic orderUnversionedTopic() {
        return TopicBuilder.name(ORDER_UNVERSIONED_TOPIC)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    }
}