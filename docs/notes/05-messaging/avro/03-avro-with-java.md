# Avro with Java and Spring Kafka

## Introduction

Java applications commonly use Avro with Kafka through serializers, deserializers, generated classes, and Schema Registry.

In a Spring Kafka application, the flow usually looks like this:

```text
Java object
     |
     v
KafkaAvroSerializer
     |
     v
Schema Registry
     |
     v
Kafka topic
```

On the consumer side:

```text
Kafka topic
     |
     v
KafkaAvroDeserializer
     |
     v
Generated Java object
```

This page focuses on the common setup using Confluent Schema Registry and generated Avro classes.

---

## Generated Classes vs Generic Records

Java applications can use Avro in two common ways.

```text
SpecificRecord
GenericRecord
```

`SpecificRecord` means Java classes are generated from Avro schemas.

`GenericRecord` means the application works with generic Avro records at runtime.

For backend services, generated classes are usually easier to work with.

They provide:

- Named Java types
- Compile-time access to fields
- Better IDE support
- Clearer application code

Generic records can be useful for tooling, routing, or systems that process many schemas dynamically.

---

## Avro Schema Files

Avro schemas are often stored as `.avsc` files.

Example:

```text
src/main/avro/OrderCreated.avsc
```

```json
{
  "type": "record",
  "name": "OrderCreated",
  "namespace": "com.example.orders.avro",
  "fields": [
    {
      "name": "orderId",
      "type": "string"
    },
    {
      "name": "customerId",
      "type": "string"
    },
    {
      "name": "amount",
      "type": "double"
    },
    {
      "name": "currency",
      "type": "string",
      "default": "USD"
    }
  ]
}
```

The `namespace` influences the generated Java package.

The `name` influences the generated Java class name.

This schema can generate a Java class similar to:

```text
com.example.orders.avro.OrderCreated
```

---

## Maven Code Generation

A Maven project can generate Java classes from `.avsc` files during the build.

Example:

```xml
<plugin>
    <groupId>org.apache.avro</groupId>
    <artifactId>avro-maven-plugin</artifactId>
    <version>${avro.version}</version>
    <executions>
        <execution>
            <phase>generate-sources</phase>
            <goals>
                <goal>schema</goal>
            </goals>
            <configuration>
                <sourceDirectory>${project.basedir}/src/main/avro</sourceDirectory>
                <outputDirectory>${project.basedir}/target/generated-sources/avro</outputDirectory>
            </configuration>
        </execution>
    </executions>
</plugin>
```

After generation, application code can use the generated class.

```java
var event = OrderCreated.newBuilder()
    .setOrderId("ORD-123")
    .setCustomerId("CUS-456")
    .setAmount(100.50)
    .setCurrency("USD")
    .build();
```

---

## Producer Dependencies

Applications commonly use Confluent's Kafka Avro serializer when integrating with Schema Registry.

Conceptually:

```text
KafkaAvroSerializer
    |
    +--> registers or looks up schema
    |
    +--> writes schema ID and Avro payload
```

The exact dependency versions should be aligned with the Kafka client, Schema Registry, and platform versions used by the project.

---

## Producer Configuration

A Spring Boot producer can be configured like this:

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: io.confluent.kafka.serializers.KafkaAvroSerializer
      properties:
        schema.registry.url: http://localhost:8081
        auto.register.schemas: true
```

The important parts are:

- `value-serializer`: Uses the Avro serializer for record values.
- `schema.registry.url`: Tells the serializer where Schema Registry is running.
- `auto.register.schemas`: Controls whether schemas can be registered automatically.

During local development, `auto.register.schemas: true` is convenient.

For shared environments, teams may prefer to register schemas through CI/CD and disable automatic registration in the application.

---

## Publishing an Avro Event

With a generated class, publishing looks similar to publishing any other Kafka record.

```java
@Service
public class OrderEventProducer {

    private final KafkaTemplate<String, OrderCreated> kafkaTemplate;

    public OrderEventProducer(KafkaTemplate<String, OrderCreated> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(Order order) {
        var event = OrderCreated.newBuilder()
            .setOrderId(order.id())
            .setCustomerId(order.customerId())
            .setAmount(order.amount())
            .setCurrency(order.currency())
            .build();

        kafkaTemplate.send(
            "order-created",
            event.getOrderId().toString(),
            event
        );
    }
}
```

The application code works with a Java object.

The serializer handles Avro encoding and Schema Registry interaction.

---

## Consumer Configuration

A Spring Boot consumer can be configured like this:

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: order-projection-service
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: io.confluent.kafka.serializers.KafkaAvroDeserializer
      properties:
        schema.registry.url: http://localhost:8081
        specific.avro.reader: true
```

The important parts are:

- `value-deserializer`: Uses the Avro deserializer for record values.
- `schema.registry.url`: Allows the deserializer to fetch writer schemas.
- `specific.avro.reader`: Returns generated Avro classes instead of generic records.

Without `specific.avro.reader: true`, the consumer may receive `GenericRecord` values instead of generated classes.

---

## Consuming an Avro Event

With generated classes and `specific.avro.reader: true`, a listener can receive the generated Avro type.

```java
@Component
public class OrderCreatedConsumer {

    @KafkaListener(
        topics = "order-created",
        groupId = "order-projection-service"
    )
    public void consume(OrderCreated event) {
        var orderId = event.getOrderId().toString();
        var customerId = event.getCustomerId().toString();
        var amount = event.getAmount();
        var currency = event.getCurrency().toString();

        // update projection
    }
}
```

Generated Avro string fields may use Avro string-like types depending on code generation settings.

Application code should avoid assuming too much about generated implementation details.

Mapping Avro events into internal application models can keep business code cleaner.

---

## Mapping Avro to Domain Objects

Generated Avro classes are transport models.

They represent the Kafka event contract.

They should not automatically become domain models.

For example:

```java
public OrderCreatedMessage toMessage(OrderCreated event) {
    return new OrderCreatedMessage(
        event.getOrderId().toString(),
        event.getCustomerId().toString(),
        BigDecimal.valueOf(event.getAmount()),
        event.getCurrency().toString()
    );
}
```

This creates a boundary between:

```text
Kafka contract model
        |
        v
Application model
```

That boundary is useful when schemas evolve.

---

## Error Handling

Avro deserialization can fail before the listener method is invoked.

Examples include:

- Schema Registry is unavailable.
- The schema ID cannot be found.
- The payload does not match the expected schema.
- The consumer is not configured for specific records when the listener expects a generated type.

This means listener-level `try/catch` blocks are not enough.

Consumer error handling should account for failures in the deserialization and listener container pipeline.

This follows the same principle covered in [Kafka Consumer](../03-kafka-consumer.md).

---

## Local Development

A local Kafka Avro setup usually needs:

```text
Kafka broker
Schema Registry
Application
```

Conceptually:

```text
Spring Boot App
      |
      +--> Kafka broker
      |
      +--> Schema Registry
```

The broker stores records.

Schema Registry stores schemas.

Both must be reachable by the application.

---

## Common Mistakes

### Treating generated Avro classes as domain models

Generated Avro classes represent message contracts.

Domain code is often cleaner when Avro objects are mapped into application-specific models.

### Forgetting specific.avro.reader

Without `specific.avro.reader: true`, consumers may receive `GenericRecord` values instead of generated classes.

### Letting every producer auto-register schemas in shared environments

Automatic registration can allow accidental schema changes.

Schema changes for shared events should usually be reviewed and validated before deployment.

### Ignoring Schema Registry availability

Consumers may need Schema Registry to deserialize records, especially when schemas are not already cached.

### Assuming Avro replaces validation

Avro validates structure and types.

It does not replace business validation.

---

## Interview Corner

### What is a generated Avro class?

It is a Java class generated from an Avro schema.

Applications can use it as a strongly typed representation of an Avro record.

### What is the difference between SpecificRecord and GenericRecord?

`SpecificRecord` uses generated Java classes.

`GenericRecord` represents records dynamically without a generated class.

### What does KafkaAvroSerializer do?

It serializes Java objects into Avro bytes and integrates with Schema Registry to register or look up schemas.

### What does specific.avro.reader=true do?

It tells the Avro deserializer to return generated Avro classes when possible.

Without it, the consumer may receive generic Avro records.

### Should Avro generated classes be used as domain models?

Usually no.

They are transport models that represent event contracts.

Mapping them into domain or application models keeps boundaries clearer.

---

## Rules of Thumb

- Store Avro schemas in source control.
- Generate Java classes from schemas during the build.
- Use generated classes for normal backend services.
- Use generic records for dynamic tooling or schema-agnostic processing.
- Configure producers and consumers with Schema Registry.
- Use `specific.avro.reader: true` when listeners expect generated classes.
- Treat generated Avro classes as message contracts, not domain models.
- Keep schema registration deliberate in shared environments.
- Handle failures that occur before listener invocation.
