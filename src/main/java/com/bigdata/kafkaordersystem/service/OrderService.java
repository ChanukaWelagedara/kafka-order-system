package com.bigdata.kafkaordersystem.service;

import com.bigdata.kafkaordersystem.dto.OrderRequest;

public interface OrderService {
    void createOrder(OrderRequest request);
}
