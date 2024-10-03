package space.zeinab.demo.kafka.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import space.zeinab.demo.kafka.config.KafkaConfig;
import space.zeinab.demo.kafka.model.Customer;
import space.zeinab.demo.kafka.model.Order;

import java.time.LocalDateTime;

@Slf4j
public class MockOutOfOrderDataProducer {

    public static void main(String[] args) {
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

        publishDummyDataForCurrentTime(objectMapper, 0);
        publishDummyDataForCurrentTimePlus15Min(objectMapper, 15);
        publishDummyDataForCurrentTimePlus20Min(objectMapper, 20);
        publishDummyDataForCurrentTimeMinus10Min(objectMapper, -10);
    }

    private static void publishDummyDataForCurrentTime(ObjectMapper objectMapper, long delay) {
        var customer = new Customer("customer1", "Alice");
        var order = new Order("customer1", 100L, LocalDateTime.now());
        try {
            String customerJSON = objectMapper.writeValueAsString(customer);
            ProducerUtil.produceRecord(KafkaConfig.CUSTOMER_VERSIONED_TOPIC, customer.customerId(), customerJSON, delay);
            ProducerUtil.produceRecord(KafkaConfig.CUSTOMER_UNVERSIONED_TOPIC, customer.customerId(), customerJSON, delay);
            log.info("Published customer data for currentTime");

            String orderJSON = objectMapper.writeValueAsString(order);
            ProducerUtil.produceRecord(KafkaConfig.ORDER_VERSIONED_TOPIC, order.orderId(), orderJSON, delay);
            ProducerUtil.produceRecord(KafkaConfig.ORDER_UNVERSIONED_TOPIC, order.orderId(), orderJSON, delay);
            log.info("Published order data for currentTime");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void publishDummyDataForCurrentTimePlus15Min(ObjectMapper objectMapper, long delay) {
        var customer = new Customer("customer1", "Alice-update");
        try {
            String customerJSON = objectMapper.writeValueAsString(customer);
            ProducerUtil.produceRecord(KafkaConfig.CUSTOMER_VERSIONED_TOPIC, customer.customerId(), customerJSON, delay);
            ProducerUtil.produceRecord(KafkaConfig.CUSTOMER_UNVERSIONED_TOPIC, customer.customerId(), customerJSON, delay);
            log.info("Published customer data For currentTime plus 15 min");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void publishDummyDataForCurrentTimePlus20Min(ObjectMapper objectMapper, long delay) {
        var order = new Order("customer1", 300L, LocalDateTime.now());
        try {
            String orderJSON = objectMapper.writeValueAsString(order);
            ProducerUtil.produceRecord(KafkaConfig.ORDER_VERSIONED_TOPIC, order.orderId(), orderJSON, delay);
            ProducerUtil.produceRecord(KafkaConfig.ORDER_UNVERSIONED_TOPIC, order.orderId(), orderJSON, delay);
            log.info("Published order data for currentTime plus 20  min");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void publishDummyDataForCurrentTimeMinus10Min(ObjectMapper objectMapper, long delay) {
        var order = new Order("customer1", 200L, LocalDateTime.now());
        try {
            String orderJSON = objectMapper.writeValueAsString(order);
            ProducerUtil.produceRecord(KafkaConfig.ORDER_VERSIONED_TOPIC, order.orderId(), orderJSON, delay);
            ProducerUtil.produceRecord(KafkaConfig.ORDER_UNVERSIONED_TOPIC, order.orderId(), orderJSON, delay);
            log.info("Published order data for currentTime minus 10  min(out-of-order data)");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}