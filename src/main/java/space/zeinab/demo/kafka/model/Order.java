package space.zeinab.demo.kafka.model;

import java.time.LocalDateTime;

public record Order(String orderId, Long orderAmount, LocalDateTime orderDateTime) {
}