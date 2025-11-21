//package com.bigdata.kafkaordersystem.kafka;
//
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.kafka.annotation.KafkaListener;
//import org.springframework.stereotype.Service;
//
//@Service
//public class DlqConsumerService {
//
//    private static final Logger log = LoggerFactory.getLogger(DlqConsumerService.class);
//
//    @KafkaListener(topics = "${app.kafka.dlq-topic}", groupId = "dlq-group")
//    public void consumeFromDlq(ConsumerRecord<String, Object> record) {
//        log.error("DLQ MESSAGE RECEIVED → key={}, value={}, partition={}, offset={}",
//                record.key(), record.value(), record.partition(), record.offset());
//    }
//}

/// ///////////////////////////////////////////////////////////////////


//
//package com.bigdata.kafkaordersystem.kafka;
//
//import com.bigdata.kafkaordersystem.avro.Order;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.kafka.annotation.KafkaListener;
//import org.springframework.stereotype.Service;
//
//@Service
//public class DlqConsumerService {
//
//    private static final Logger log = LoggerFactory.getLogger(DlqConsumerService.class);
//
//    @KafkaListener(
//            topics = "${app.kafka.dlq-topic}",
//            groupId = "dlq-group",
//            containerFactory = "dlqKafkaListenerContainerFactory"
//    )
//    public void consumeFromDlq(ConsumerRecord<String, Order> record) {
//        log.error(
//                "💀 DLQ MESSAGE RECEIVED → key={}, orderId={}, product={}, price={}, partition={}, offset={}",
//                record.key(),
//                record.value().getOrderId(),
//                record.value().getProduct(),
//                record.value().getPrice(),
//                record.partition(),
//                record.offset()
//        );
//
//        // Handle the failed order
//        handleFailedOrder(record.value(), record);
//    }
//
//    private void handleFailedOrder(Order order, ConsumerRecord<String, Order> record) {
//        // Implement your DLQ handling logic here (persist to DB, send alert, manual review queue, etc.)
//        log.warn("Handling failed order from DLQ: orderId={}, originalOffset={}, reason=Processing failed after retries",
//                order.getOrderId(), record.offset());
//
//
//    }
//}


package com.bigdata.kafkaordersystem.kafka;

import com.bigdata.kafkaordersystem.avro.Order;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class DlqConsumerService {

    private static final Logger log = LoggerFactory.getLogger(DlqConsumerService.class);

    @KafkaListener(
            topics = "${app.kafka.dlq-topic}",
            groupId = "dlq-group",
            containerFactory = "dlqKafkaListenerContainerFactory"
    )
    public void consumeFromDlq(ConsumerRecord<String, Order> record) {
        log.error(
                "DLQ MESSAGE RECEIVED → key={}, orderId={}, product={}, price={}, partition={}, offset={}",
                record.key(),
                record.value().getOrderId(),
                record.value().getProduct(),
                record.value().getPrice(),
                record.partition(),
                record.offset()
        );

        // Handle the failed order
        handleFailedOrder(record.value(), record);
    }

    private void handleFailedOrder(Order order, ConsumerRecord<String, Order> record) {
        // Persist to DB, send alert, manual review, etc.
        log.warn("Handling failed order from DLQ: orderId={}, originalOffset={}, reason=Processing failed after retries",
                order.getOrderId(), record.offset());
    }
}
