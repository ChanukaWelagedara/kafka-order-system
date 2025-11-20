package com.bigdata.kafkaordersystem.kafka;
import com.bigdata.kafkaordersystem.avro.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderConsumerService {

    private final Logger log = LoggerFactory.getLogger(OrderConsumerService.class);
    private float totalPrice = 0;
    private int orderCount = 0;

    @KafkaListener(topics = "orders", groupId = "order-group")
    public void consume(Order order) {
        log.info("Received: {}", order);
        // simulate transient error
        if ((int)(Math.random()*10) % 2 == 0) throw new RuntimeException("Temporary error");

        totalPrice += order.getPrice();
        orderCount++;
        log.info("Processed order {} | Running avg: {}", order.getOrderId(), totalPrice / orderCount);
    }
}