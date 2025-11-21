package com.bigdata.kafkaordersystem.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class DlqConsumerService {

    private static final Logger log = LoggerFactory.getLogger(DlqConsumerService.class);

    @KafkaListener(topics = "${app.kafka.dlq-topic}", groupId = "dlq-group")
    public void consumeFromDlq(ConsumerRecord<String, Object> record) {
        log.error("DLQ MESSAGE RECEIVED → key={}, value={}, partition={}, offset={}",
                record.key(), record.value(), record.partition(), record.offset());
    }
}
