package com.bigdata.kafkaordersystem.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderRequest {
    @NotBlank(message = "orderId is required")
    private String orderId;

    @NotBlank(message = "product is required")
    private String product;

    @NotNull(message = "price is required")
    @Positive(message = "price must be positive")
    private float price;


}
