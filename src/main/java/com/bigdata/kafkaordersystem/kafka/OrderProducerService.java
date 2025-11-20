package com.bigdata.kafkaordersystem.kafka;

import com.bigdata.kafkaordersystem.avro.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class OrderProducerService {
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final Logger log = LoggerFactory.getLogger(OrderProducerService.class);
    private final String topic = "orders";

    public OrderProducerService(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendOrder(Order order) {
        kafkaTemplate.send(topic, order.getOrderId().toString(), order)
                .whenComplete((result, ex) -> {
                    if (ex == null) log.info("Order sent: {}", order);
                    else log.error("Failed sending order: {}", order, ex);
                });
    }
}
