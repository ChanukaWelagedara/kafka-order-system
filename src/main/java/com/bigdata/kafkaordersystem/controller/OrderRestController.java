package com.bigdata.kafkaordersystem.controller;
import com.bigdata.kafkaordersystem.dto.ApiResponse;
import com.bigdata.kafkaordersystem.dto.OrderRequest;
import com.bigdata.kafkaordersystem.service.OrderService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class OrderRestController {

    private final OrderService orderService;

    @PostMapping
    public ResponseEntity<ApiResponse> createOrder(@Valid @RequestBody OrderRequest request) {
        orderService.createOrder(request);
        return ResponseEntity.ok(new ApiResponse("200", "Order created successfully", null, true));
    }
}