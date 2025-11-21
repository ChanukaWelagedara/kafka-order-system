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

        // Simulate a transient error for retries
        boolean fail = Math.random() < 0.5; // 50% chance to fail
        if (fail) {
            log.warn("Retrying record {} attempt ...", order.getOrderId());
            throw new RuntimeException("Temporary error");
        }

        // Successful processing
        totalPrice += order.getPrice();
        orderCount++;
        float runningAvg = totalPrice / orderCount;
        log.info("Processed order {} | Running avg: {}", order.getOrderId(), runningAvg);
    }
}


//package com.bigdata.kafkaordersystem.kafka;
//
//import com.bigdata.kafkaordersystem.avro.Order;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.kafka.annotation.KafkaListener;
//import org.springframework.stereotype.Service;
//
//@Service
//public class OrderConsumerService {
//
//    private final Logger log = LoggerFactory.getLogger(OrderConsumerService.class);
//    private float totalPrice = 0;
//    private int orderCount = 0;
//
//    @KafkaListener(topics = "orders", groupId = "order-group")
//    public void consume(Order order) {
//        log.info("Received: {}", order);
//
//        // Simulate transient error randomly
//        if ((int)(Math.random() * 10) % 2 == 0) {
//            log.info("Simulating temporary error for order {}", order.getOrderId());
//            throw new RuntimeException("Temporary error");
//        }
//
//        // Aggregate and log running average
//        totalPrice += order.getPrice();
//        orderCount++;
//        log.info("Processed order {} | Running avg: {}", order.getOrderId(), totalPrice / orderCount);
//    }
//}
//
////package com.bigdata.kafkaordersystem.kafka;
////import com.bigdata.kafkaordersystem.avro.Order;
////import org.slf4j.Logger;
////import org.slf4j.LoggerFactory;
////import org.springframework.kafka.annotation.KafkaListener;
////import org.springframework.stereotype.Service;
////
////@Service
////public class OrderConsumerService {
////
////    private final Logger log = LoggerFactory.getLogger(OrderConsumerService.class);
////    private float totalPrice = 0;
////    private int orderCount = 0;
////
////    @KafkaListener(topics = "orders", groupId = "order-group")
////    public void consume(Order order) {
////        log.info("Received: {}", order);
////        // simulate transient error
////        if ((int)(Math.random()*10) % 2 == 0) throw new RuntimeException("Temporary error");
////
////        totalPrice += order.getPrice();
////        orderCount++;
////        log.info("Processed order {} | Running avg: {}", order.getOrderId(), totalPrice / orderCount);
////    }
////}