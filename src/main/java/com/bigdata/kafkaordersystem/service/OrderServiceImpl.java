package com.bigdata.kafkaordersystem.service;
import com.bigdata.kafkaordersystem.dto.OrderRequest;
import com.bigdata.kafkaordersystem.kafka.OrderProducerService;
import com.bigdata.kafkaordersystem.avro.Order;
import org.springframework.stereotype.Service;

@Service
public class OrderServiceImpl implements OrderService {

    private final OrderProducerService producer;

    public OrderServiceImpl(OrderProducerService producer) {
        this.producer = producer;
    }

    @Override
    public void createOrder(OrderRequest request) {
        Order order = Order.newBuilder()
                .setOrderId(request.getOrderId())
                .setProduct(request.getProduct())
                .setPrice(request.getPrice())
                .setQuantity(request.getQuantity())
                .build();
        producer.sendOrder(order);
    }
}