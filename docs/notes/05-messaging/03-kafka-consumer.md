# Kafka Consumer

## Introduction

A Kafka consumer is an application that reads and processes records from Kafka topics.

While producers are responsible for reliably publishing records to Kafka, consumers are responsible for processing those records reliably.

Consumer applications need to consider more than simply reading a message.

They must also handle questions such as:

- How are records converted into application objects?
- What happens when a record cannot be deserialized?
- What happens when processing fails?
- Which failures should be retried?
- What happens after retries are exhausted?
- How can multiple consumers process records in parallel?
- How can duplicate processing be prevented?

Spring Kafka provides abstractions for implementing these behaviors while still relying on Kafka's underlying consumer model.

---

## Consuming Records with Spring Kafka

Spring Kafka provides `@KafkaListener` for declaring methods or classes that consume records from Kafka topics.

A simple listener can be declared directly on a method:

```java
@KafkaListener(
    topics = "product-created",
    groupId = "product-service"
)
public void consume(ProductCreatedEvent event) {
    // process event
}
```

When a record becomes available, Spring Kafka:

1. Polls the record from Kafka.
2. Deserializes the record.
3. Converts it into the expected Java type.
4. Invokes the listener method.

This allows application code to focus primarily on processing the event.

---

## @KafkaListener and @KafkaHandler

`@KafkaListener` can also be placed at the class level.

Individual handler methods can then use `@KafkaHandler`.

```java
@KafkaListener(topics = "product-created")
@Component
public class ProductEventConsumer {

    @KafkaHandler
    public void handle(ProductCreatedEvent event) {
        // process event
    }
}
```

This approach becomes useful when a listener handles multiple event types.

For example:

```java
@KafkaListener(topics = "product-events")
@Component
public class ProductEventConsumer {

    @KafkaHandler
    public void handle(ProductCreatedEvent event) {
        // handle product creation
    }

    @KafkaHandler
    public void handle(ProductUpdatedEvent event) {
        // handle product update
    }
}
```

Spring selects the appropriate handler based on the converted payload type.

If only one event type is being consumed, placing `@KafkaListener` directly on the method is often simpler.

---

## Consumer Configuration

A consumer needs several properties to connect to Kafka and deserialize records.

For example:

```java
private Map<String, Object> consumerConfig() {
    return Map.of(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
        environment.getRequiredProperty("spring.kafka.bootstrap-servers"),

        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class,

        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        ErrorHandlingDeserializer.class,

        ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS,
        JacksonJsonDeserializer.class,

        ConsumerConfig.GROUP_ID_CONFIG,
        environment.getRequiredProperty("spring.kafka.consumer.group-id"),

        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
        environment.getRequiredProperty("spring.kafka.consumer.auto-offset-reset"),

        "spring.json.trusted.packages",
        "*"
    );
}
```

Important consumer properties include:

| Property | Purpose |
| --- | --- |
| `bootstrap.servers` | Kafka brokers used to establish the initial connection |
| `key.deserializer` | Converts the record key into a Java object |
| `value.deserializer` | Converts the record value into a Java object |
| `group.id` | Identifies the consumer group |
| `auto.offset.reset` | Determines where consumption begins when no committed offset exists |

---

## The Serialization Boundary

Kafka does not transport Java objects directly.

A producer serializes an object into bytes before sending it to Kafka.

The consumer must deserialize those bytes back into something the application understands.

Conceptually:

```text
Producer

ProductCreatedEvent
        |
        v
    Serializer
        |
        v
      bytes
        |
        v
      Kafka
        |
        v
      bytes
        |
        v
   Deserializer
        |
        v
ProductCreatedEvent

Consumer
```

This creates an important boundary between producers and consumers.

---

### Sharing Event Classes

One approach is to place event contracts in a shared module.

For example:

```text
core
└── ProductCreatedEvent

command
└── depends on core

sink
└── depends on core
```

Both producer and consumer then use the exact same Java class.

This can be convenient in a sandbox, modular application, or tightly controlled environment.

However, sharing Java classes between independently deployed microservices introduces compile-time coupling.

An alternative is for producer and consumer applications to maintain their own models while agreeing on the serialized event contract.

For example:

```text
Producer Model
      |
      v
 JSON / Avro / Protobuf
      |
      v
Consumer Model
```

The important contract is the data exchanged between the applications, not necessarily the Java class itself.

---

## Deserialization Failures

Deserialization happens before the listener can process the event.

For example:

```text
Kafka
  |
  v
Deserialize
  |
  X failure
  |
  v
@KafkaListener never invoked
```

This is different from an exception thrown by business logic inside the listener.

A malformed record or incompatible event type can therefore fail before application processing begins.

Without appropriate error handling, the consumer may repeatedly encounter the same record and continuously produce errors.

---

## ErrorHandlingDeserializer

Spring Kafka provides `ErrorHandlingDeserializer` to make deserialization failures available to the listener container's error-handling infrastructure.

Instead of configuring the JSON deserializer directly:

```text
Kafka
   |
   v
JacksonJsonDeserializer
```

the actual deserializer can be wrapped:

```text
Kafka
   |
   v
ErrorHandlingDeserializer
   |
   v
JacksonJsonDeserializer
```

For example:

```java
ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
ErrorHandlingDeserializer.class,

ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS,
JacksonJsonDeserializer.class
```

`JacksonJsonDeserializer` is still responsible for converting the JSON into a Java object.

`ErrorHandlingDeserializer` acts as a wrapper that captures deserialization failures so Spring Kafka's error-handling infrastructure can process them appropriately.

---

### Why This Matters

Consider an invalid record:

```json
{
  "productId": "123",
  "price": "not-a-number"
}
```

If the consumer expects:

```java
BigDecimal price
```

deserialization may fail.

The failure happens before:

```java
@KafkaHandler
public void handle(ProductCreatedEvent event) {
    ...
}
```

can be called.

This is why logging or exception handling only inside the listener is not sufficient for every Kafka consumer failure.

---

## Consumer Error Handling

Spring Kafka provides `DefaultErrorHandler` for handling failures during consumer processing.

For example:

```java
var errorHandler = new DefaultErrorHandler(
    new DeadLetterPublishingRecoverer(kafkaTemplate),
    new FixedBackOff(5000, 3)
);
```

The error handler can determine whether the record should:

- Be retried
- Be recovered
- Be published to a Dead Letter Topic

Conceptually:

```text
Kafka Record
     |
     v
Consumer
     |
     X failure
     |
     v
DefaultErrorHandler
     |
     ├── Retry
     |
     └── Recover
```

---

## Dead Letter Topics

A **Dead Letter Topic (DLT)** provides a destination for records that cannot be successfully processed and that the configured error-handling strategy has decided to recover rather than continue retrying.

Spring Kafka provides `DeadLetterPublishingRecoverer` for publishing failed records to a DLT.

```java
new DeadLetterPublishingRecoverer(kafkaTemplate)
```

With the default naming strategy, a failed record from:

```text
product-created
```

can be recovered to:

```text
product-created-DLT
```

Conceptually:

```text
product-created
      |
      v
   Consumer
      |
      X processing failed
      |
      v
   retries
      |
      X still failing
      |
      v
product-created-DLT
```

A DLT prevents permanently failing records from continuously blocking normal processing.

It also preserves failed records so they can be investigated or potentially reprocessed later.

---

### DLT Does Not Mean Every Failure

A processing failure does not automatically mean that the record should immediately be published to a DLT.

For example:

```text
Processing failure
       |
       v
     retry
       |
       v
    success
```

No DLT is required.

A DLT becomes relevant when the error-handling strategy determines that processing should no longer continue normally.

This can happen because:

- The failure is considered non-retryable.
- The configured retries have been exhausted.

---

## Retryable and Non-Retryable Failures

Not every failure should be treated the same way.

Some failures are temporary.

For example:

```text
Database temporarily unavailable
External API returns 503
Network timeout
Temporary infrastructure failure
```

Trying the operation again later may succeed.

These are **retryable failures**.

Other failures are unlikely to change regardless of how many times the same operation is attempted.

For example:

```text
Invalid business input
Unsupported operation
Permanently invalid state
Malformed data
```

These are generally **non-retryable failures**.

---

### Exception Classification

Applications can express these differences through exception types.

For example:

```java
throw new RetryableException(...);
```

or:

```java
throw new NonRetryableException(...);
```

The error handler can then classify them:

```java
errorHandler.addRetryableExceptions(
    RetryableException.class
);

errorHandler.addNotRetryableExceptions(
    NonRetryableException.class
);
```

This gives the application control over which failures deserve another attempt.

The important principle is:

> Retryability is often a business or application decision, not simply a Kafka decision.

---

## Retry Backoff

Retries should generally not happen continuously in a tight loop.

Spring Kafka allows a backoff strategy to control the delay and number of retries.

For example:

```java
new FixedBackOff(5000, 3)
```

This means:

```text
Initial attempt
      |
      X
      |
   wait 5s
      |
Retry #1
      |
      X
      |
   wait 5s
      |
Retry #2
      |
      X
      |
   wait 5s
      |
Retry #3
```

The value `3` represents three retry attempts after the initial delivery attempt.

If processing still fails after the retries are exhausted, the configured recoverer can take over.

With a `DeadLetterPublishingRecoverer`:

```text
Initial attempt ❌
       |
Retry #1 ❌
       |
Retry #2 ❌
       |
Retry #3 ❌
       |
       v
      DLT
```

---

## Transactions and Consumer Retries

Database transactions and Kafka consumer retries solve different problems.

A database transaction controls what happens to database changes when processing fails.

For example:

```text
Kafka Event
    |
    v
@Transactional
    |
    ├── INSERT product      ✓
    ├── UPDATE inventory    X
    |
    v
ROLLBACK
```

The Kafka error handler determines what happens to the Kafka record afterward.

```text
Processing failure
      |
      v
DefaultErrorHandler
      |
      ├── retry
      |
      └── DLT
```

These concerns should not be confused.

```text
@Transactional
    ↓
What happens to database changes?


DefaultErrorHandler
    ↓
What happens to the failed Kafka delivery?
```

More complex transaction coordination between Kafka and databases is covered separately.

---

## Consumer Groups

A **consumer group** allows multiple consumer instances to cooperate when processing a topic.

Consumers belong to the same group when they use the same:

```text
group.id
```

Kafka distributes topic partitions among the consumers in that group.

For example:

```text
Topic: product-created

Partition 0
Partition 1
Partition 2


Consumer Group: product-service

Consumer 1 → Partition 0
Consumer 2 → Partition 1
Consumer 3 → Partition 2
```

This allows multiple partitions to be processed in parallel.

---

### Partition Assignment

Within a consumer group, a partition is assigned to one consumer at a time.

For example:

```text
3 partitions
2 consumers

Consumer 1 → Partition 0
             Partition 1

Consumer 2 → Partition 2
```

A consumer can own multiple partitions.

However, the same partition is not simultaneously assigned to multiple consumers within the same consumer group.

This preserves partition-level ordering while allowing parallel processing across partitions.

---

## Consumer Parallelism

The number of partitions places an upper bound on useful consumer parallelism within a consumer group.

Consider:

```text
3 partitions
3 consumers
```

Kafka can assign:

```text
Consumer 1 → P0
Consumer 2 → P1
Consumer 3 → P2
```

All three consumers can perform useful work.

Now consider:

```text
3 partitions
5 consumers
```

The assignment may look like:

```text
Consumer 1 → P0
Consumer 2 → P1
Consumer 3 → P2
Consumer 4 → idle
Consumer 5 → idle
```

The additional consumers do not increase processing parallelism because there are no additional partitions to assign.

A useful rule is:

```text
Maximum useful consumer parallelism
within a consumer group

≈ number of partitions
```

---

## Consumer Rebalancing

Consumer group membership can change over time.

For example, a consumer may:

- Start
- Stop
- Crash
- Lose connectivity
- Be scaled up or down

Kafka responds by redistributing partition ownership among the available consumers.

This process is called **rebalancing**.

For example:

```text
BEFORE

Consumer 1 → P0
Consumer 2 → P1
Consumer 3 → P2
```

If Consumer 2 becomes unavailable:

```text
Consumer 1 → P0
Consumer 2 → DOWN
Consumer 3 → P2
```

Kafka can rebalance the group:

```text
AFTER REBALANCE

Consumer 1 → P0 + P1
Consumer 3 → P2
```

This allows the remaining consumers to continue processing the workload.

---

## Multiple Consumer Groups

Consumer groups are independent from one another.

Consider two applications consuming the same topic:

```text
                product-created
                P0    P1    P2
                 |     |     |
          ┌──────┴─────┴─────┴──────┐
          |                           |
          v                           v

Group: inventory              Group: analytics

Consumer A → P0               Consumer X → P0
Consumer B → P1               Consumer Y → P1
Consumer C → P2               Consumer Z → P2
```

Both groups can process the records independently.

This creates an important distinction:

```text
Same consumer group
        ↓
Work is distributed between consumers.


Different consumer groups
        ↓
Each group consumes the stream independently.
```

For example:

```text
OrderCreated
     |
     ├── inventory-service
     |
     ├── notification-service
     |
     └── analytics-service
```

Each service can use its own consumer group and independently process the same event.

Multiple instances of the same service can then share a group ID to distribute that service's workload.

---

## Idempotent Consumers

Kafka consumers must be designed with duplicate delivery in mind.

A record may be processed more than once because of failures, retries, or redelivery.

An **idempotent consumer** ensures that processing the same logical message multiple times does not repeatedly produce the same business side effect.

For example:

```text
messageId = abc-123
```

First delivery:

```text
abc-123
   |
   v
Not processed before
   |
   v
Process business operation
   |
   v
Store abc-123
```

Duplicate delivery:

```text
abc-123
   |
   v
Already processed
   |
   v
Skip
```

The consumer can therefore safely receive the same logical message multiple times.

---

### Idempotency Key

One approach is to assign every logical message a unique identifier.

For example, a producer can include:

```text
messageId = 550e8400-e29b-41d4-a716-446655440000
```

as a Kafka header.

```java
producerRecord.headers().add(
    "messageId",
    messageId.getBytes()
);
```

The consumer can then retrieve the header:

```java
@Header(name = "messageId")
String messageId
```

and use it when processing the event.

The important requirement is that redelivery of the **same logical message must retain the same idempotency identifier**.

Generating a new identifier for every retry or republication would prevent the consumer from recognizing the duplicate.

---

## Database-Backed Idempotency

A consumer can store processed message identifiers in a database.

For example:

```java
if (repository.existsByMessageId(messageId)) {
    return;
}

repository.save(...);
```

Conceptually:

```text
Kafka Event
    |
    v
messageId = abc-123
    |
    v
Does abc-123 exist?
    |
    ├── YES → already processed → return
    |
    └── NO
         |
         v
      process
         |
         v
      persist
```

This provides application-level duplicate detection.

---

### Database Uniqueness

An application-level existence check alone is not sufficient protection against concurrent processing.

Consider:

```text
Consumer A                    Consumer B

exists(abc)? → false          exists(abc)? → false

save abc                      save abc
```

Both consumers could observe that the identifier does not exist before either transaction commits.

The database should therefore also enforce uniqueness on the idempotency identifier.

Conceptually:

```sql
UNIQUE (message_id)
```

The database then provides the final protection against concurrent duplicate inserts.

The application check remains useful for avoiding unnecessary processing and providing clearer application behavior.

---

## Idempotency and Transactions

The idempotency marker and the business operation should ideally participate in the same database transaction.

Consider:

```text
Store messageId        ✓
Perform business work  X
```

If the idempotency marker were committed independently, the next delivery could incorrectly appear to have already been successfully processed.

Instead:

```text
@Transactional

Check messageId
      |
Perform business operation
      |
Store idempotency state
      |
      X failure
      |
ROLLBACK EVERYTHING
```

The next delivery can then safely attempt the operation again.

---

## Kafka Producer Idempotence vs Consumer Idempotency

These concepts solve different problems.

### Producer Idempotence

Producer idempotence protects against duplicate records caused by Kafka Producer's internal retries.

```text
Application calls send() once

        |
        v

Kafka Producer
   |
   ├── attempt
   ├── retry
   └── retry

        |
        v

Avoid duplicate append caused by producer retry
```

### Consumer Idempotency

Consumer idempotency protects business processing when the same logical record is delivered more than once.

```text
Kafka
   |
   ├── delivery #1
   |
   └── delivery #2

        |
        v

Consumer recognizes same messageId
        |
        v
Business effect occurs once
```

Kafka producer idempotence does not replace consumer idempotency.

They operate at different stages of the messaging lifecycle.

---

## Kafka Headers

Consumers can access metadata associated with received Kafka records.

For example:

```java
@KafkaHandler
public void handleProductCreatedEvent(
    @Payload ProductCreatedEvent event,
    @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
    @Header(KafkaHeaders.RECEIVED_KEY) String key,
    @Header("messageId") String messageId
) {
    ...
}
```

`RECEIVED_TOPIC` and `RECEIVED_KEY` represent metadata from the consumed record.

This distinction matters because Spring also defines headers used for outbound Kafka messages.

Using the wrong header constant can cause argument resolution to fail before the listener method is invoked.

---

## Failures Before Listener Invocation

Not every consumer failure occurs inside application business logic.

The processing pipeline can be viewed as:

```text
Kafka Record
     |
     v
Deserialization
     |
     v
Message Conversion
     |
     v
Header / Argument Resolution
     |
     v
@KafkaHandler
     |
     v
Business Logic
```

A failure at any earlier stage means the application may never reach the listener method.

Examples include:

- Deserialization failures
- Missing required headers
- Message conversion failures
- Listener argument resolution failures

This means:

```java
log.error(...);
```

inside the listener cannot provide visibility into every consumer failure.

Infrastructure-level failures should also be observed through the Kafka error-handling infrastructure.

---

## Observing Retry Failures

A retry listener can provide visibility into failed processing attempts.

For example:

```java
errorHandler.setRetryListeners(
    (record, exception, deliveryAttempt) -> {
        log.error(
            "Kafka processing failed. topic={}, partition={}, offset={}, attempt={}",
            record.topic(),
            record.partition(),
            record.offset(),
            deliveryAttempt,
            exception
        );
    }
);
```

This is particularly useful for failures that occur before application business logic is reached.

Useful information to log includes:

```text
Topic
Partition
Offset
Delivery attempt
Exception type
Exception message
Stack trace
```

Without sufficient observability, an infrastructure failure can otherwise appear only as repeated retries followed by a record being recovered to the DLT.

---

## Common Mistakes

### Assuming every failure reaches the listener

Deserialization, conversion, or header-resolution failures can occur before `@KafkaListener` or `@KafkaHandler` executes.

Error handling must therefore exist outside the business listener as well.

---

### Retrying every exception

Some failures cannot succeed simply by trying again.

Retrying permanently invalid messages wastes resources and delays other processing.

Classify failures based on whether another attempt can reasonably change the result.

---

### Treating the DLT as the first response to every failure

Temporary failures may recover after retrying.

The DLT should be part of a deliberate recovery strategy rather than the automatic destination for every processing exception.

---

### Adding more consumers than partitions

Additional consumers within the same consumer group do not increase processing parallelism once every partition already has an assigned consumer.

---

### Assuming consumer groups prevent duplicate delivery

Consumer groups distribute partition ownership.

They do not provide business-level idempotency.

Consumer applications should still be designed to safely handle duplicate processing.

---

### Using only exists() for idempotency

Checking the database before inserting does not completely prevent concurrent duplicate processing.

Use a database uniqueness constraint as the final enforcement mechanism.

---

### Generating a new idempotency ID during every retry

The identifier must represent the logical message.

If every delivery receives a different identifier, duplicate detection cannot work.

---

### Confusing producer idempotence with consumer idempotency

Producer idempotence protects Kafka's publishing process.

Consumer idempotency protects application business processing.

Both may be needed in the same system.

---

## Interview Corner

### What is a Kafka consumer?

A Kafka consumer reads records from Kafka topics and processes them.

Consumers typically participate in consumer groups, allowing Kafka to distribute partitions across multiple consumer instances.

---

### What is a consumer group?

A consumer group is a collection of consumers that cooperate to process topic partitions.

Within a group, each partition is assigned to one consumer at a time.

This allows processing to be distributed across multiple consumer instances.

---

### What determines maximum consumer parallelism?

The number of partitions.

If a topic has three partitions, at most three consumers within a consumer group can actively consume those partitions at the same time.

Additional consumers remain idle until partition ownership becomes available.

---

### What happens when a consumer goes down?

Kafka detects changes in consumer group membership and can rebalance partition assignments among the remaining consumers.

This allows processing to continue without the failed consumer.

---

### What happens if two applications use different consumer groups?

Each consumer group maintains independent consumption of the topic.

This allows multiple applications to process the same stream of records for different purposes.

---

### What is a Dead Letter Topic?

A Dead Letter Topic stores records that could not be successfully processed and that the configured recovery strategy has decided not to continue processing normally.

This commonly happens when a failure is non-retryable or when retry attempts have been exhausted.

---

### What is the difference between a retryable and non-retryable exception?

A retryable exception represents a failure that may succeed on a later attempt, such as temporary service unavailability.

A non-retryable exception represents a failure where repeating the same operation is unlikely to change the result.

---

### Why use ErrorHandlingDeserializer?

Deserialization occurs before listener business logic.

`ErrorHandlingDeserializer` allows deserialization failures to participate in Spring Kafka's error-handling infrastructure rather than repeatedly failing outside the normal listener error flow.

---

### What is an idempotent consumer?

An idempotent consumer can safely process the same logical message multiple times without repeatedly applying its business side effect.

A common implementation stores a unique message identifier and skips messages that have already been successfully processed.

---

### Is checking existsByMessageId() enough for idempotency?

Not by itself.

Concurrent processing can allow two transactions to observe that the identifier does not exist.

A database uniqueness constraint should also enforce the idempotency key.

---

### Does Kafka producer idempotence make the consumer idempotent?

No.

Producer idempotence prevents duplicate records caused by producer retries.

Consumer idempotency prevents duplicate business effects when records are delivered or processed multiple times.

They solve different problems.

---

## Rules of Thumb

- Treat duplicate delivery as something consumers must be prepared to handle.
- Use consumer groups to scale processing across partitions.
- Do not create more consumer instances solely for parallelism than the available partition count can utilize.
- Expect consumer group membership changes to cause partition reassignment.
- Distinguish deserialization failures from business-processing failures.
- Use `ErrorHandlingDeserializer` when deserialization failures need to participate in Spring Kafka's error-handling flow.
- Retry failures only when another attempt has a reasonable chance of succeeding.
- Use backoff rather than immediately retrying failures in a tight loop.
- Use a DLT as part of a deliberate failure-recovery strategy.
- Make business processing idempotent when duplicate delivery is possible.
- Enforce idempotency identifiers with database constraints, not only application checks.
- Keep the idempotency marker and associated database changes atomic where possible.
- Add observability around retry and recovery behavior so infrastructure failures are visible even when the listener is never invoked.