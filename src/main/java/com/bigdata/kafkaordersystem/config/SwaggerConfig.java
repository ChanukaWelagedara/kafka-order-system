package com.bigdata.kafkaordersystem.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SwaggerConfig {

    @Bean
    public OpenAPI customOpenAPI() {
        return new OpenAPI()
                .openapi("3.0.3") // force OpenAPI version
                .info(new Info()
                        .title("Kafka Order System API")
                        .version("1.0")
                        .description("REST API for producing and consuming order messages using Kafka and Avro"));
    }
}
