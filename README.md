# Kafka Order System

A Spring Boot application that demonstrates real-time order processing using Apache Kafka with Avro serialization.

## Features

- RESTful API for order creation
- Kafka producer-consumer architecture
- Avro schema for data serialization
- Schema Registry integration
- Retry mechanism with Dead Letter Queue (DLQ)
- Real-time order analytics (running average)
- Swagger UI documentation

## Technologies

- Java 21
- Spring Boot 3.5.7
- Apache Kafka 7.8.0
- Apache Avro 1.11.1
- Confluent Schema Registry
- Docker & Docker Compose
- Maven

## Prerequisites

- JDK 21
- Maven 3.8+
- Docker Desktop

## Quick Start

### 1. Start Kafka Infrastructure

```bash
docker-compose up -d
```

### 2. Build the Application

```bash
mvn clean install
```

### 3. Run the Application

```bash
mvn spring-boot:run
```

Application runs on: `http://localhost:5000`

## API Endpoints

### Create Order

```bash
POST http://localhost:5000/orders
Content-Type: application/json

{
  "orderId": "ORD-001",
  "product": "Laptop",
  "price": 1299.99
}
```

### Swagger UI

```
http://localhost:5000/swagger-ui.html
```

## Monitoring

- **Kafka Control Center**: http://localhost:9021
- **Schema Registry**: http://localhost:5001

## Project Structure

```
src/main/java/com/bigdata/kafkaordersystem/
├── config/              # Kafka & Swagger configuration
├── controller/          # REST API controllers
├── dto/                 # Data transfer objects
├── exception/           # Exception handlers
├── kafka/               # Producer, Consumer, DLQ services
└── service/             # Business logic

src/main/resources/
├── application.properties
└── order.avsc          # Avro schema
```

## Configuration

Key settings in `application.properties`:

- Server Port: `5000`
- Kafka Broker: `localhost:29092`
- Schema Registry: `http://localhost:5001`
- Order Topic: `orders`
- DLQ Topic: `orders-dlq`

## How It Works

1. Client sends order via REST API
2. Producer publishes order to Kafka topic
3. Consumer processes the order (with simulated retry logic)
4. Successfully processed orders update running analytics
5. Failed orders after retries go to Dead Letter Queue
6. DLQ consumer logs failed messages for investigation
