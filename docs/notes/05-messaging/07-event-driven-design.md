# Event-Driven Design

## Introduction

Kafka concepts become useful when they are applied to system design.

An event-driven design is not only about publishing messages.

It requires decisions about:

- Topic boundaries
- Event names
- Message keys
- Partitioning
- Schemas
- Compatibility
- Retries
- Dead letter topics
- Idempotency
- Observability

Each decision affects reliability, scalability, and how easily services can evolve.

---

## Start with the Business Event

A good Kafka event represents something meaningful that happened.

Examples:

```text
OrderCreated
PaymentCaptured
InventoryReserved
ShipmentDispatched
```

These are facts.

They describe completed changes in the business.

Avoid vague event names such as:

```text
OrderMessage
OrderData
OrderPayload
ProcessOrder
```

These names do not clearly communicate what happened.

The event name should help consumers understand why the event exists.

---

## Event Notification vs Event-Carried State

Events can contain different levels of detail.

An event notification only says that something happened.

```json
{
  "orderId": "ORD-123"
}
```

Consumers may need to call the producer service to fetch details.

Event-carried state includes the useful state in the event itself.

```json
{
  "orderId": "ORD-123",
  "customerId": "CUS-456",
  "status": "CREATED",
  "totalAmount": "100.50",
  "currency": "USD"
}
```

This reduces direct service calls and allows consumers to work more independently.

The trade-off is that the event contract becomes more important.

More fields mean more schema evolution responsibility.

---

## Topic Design

A topic is a stream of related records.

Topic design should be based on how events are produced, consumed, retained, and evolved.

Example:

```text
order-created
payment-captured
inventory-reserved
```

This design uses one topic per event type.

Another approach is a broader event stream:

```text
order-events
```

containing:

```text
OrderCreated
OrderCancelled
OrderShipped
```

Both approaches can work.

The decision depends on:

- Whether consumers usually need all event types or only one
- Whether events share retention requirements
- Whether events share schema evolution rules
- Whether ordering is needed across event types
- Whether the topic will become too broad to manage clearly

Avoid putting unrelated events into the same topic just because they are produced by the same service.

---

## Message Keys

The message key influences partition selection.

Records with the same key are normally routed to the same partition while the partition count is stable.

```text
key = order-123

OrderCreated      -> Partition 1
PaymentCaptured   -> Partition 1
OrderShipped      -> Partition 1
```

Use a key that matches the ordering requirement.

If events for the same order must be processed in order, use `orderId`.

If events for the same account must be processed in order, use `accountId`.

The key should not be chosen randomly.

It is part of the event design.

---

## Partitioning Strategy

Partitions provide parallelism.

They also define the scope of Kafka's ordering guarantee.

```text
More partitions
        |
        +--> more potential consumer parallelism
        +--> more partition management overhead
        +--> no global ordering across the topic
```

Choose the partition count based on expected throughput, consumer parallelism, and operational headroom.

Do not rely on adding partitions casually if strict key-based ordering is important.

Changing partition count can affect how keys are distributed.

That can affect ordering assumptions for records with the same key across the change.

---

## Schema Design

Schemas are service contracts.

For Avro-based topics, design schemas with evolution in mind.

Rules of thumb:

- Use clear field names.
- Add fields with defaults.
- Avoid changing field meanings.
- Avoid casual field renames.
- Use nullable fields deliberately.
- Be careful with enums that may expand.
- Use logical types for timestamps, dates, UUIDs, and exact decimals.

Event schemas should represent what consumers need to know, not every internal field in the producer's domain model.

Internal models can change more frequently than public event contracts.

---

## Compatibility Strategy

Shared events should have a compatibility strategy.

Common options include:

```text
BACKWARD
FORWARD
FULL
```

For shared business events, full compatibility is often a good goal because producers and consumers are deployed independently.

```text
New consumers can read old data.
Old consumers can read new data.
```

Compatibility checks should happen before a schema reaches production.

Schema Registry can enforce compatibility within a subject.

However, schema compatibility does not replace business review.

A schema can be technically compatible but still semantically confusing.

---

## Consumer Design

Consumers should be designed for duplicate delivery.

Common consumer responsibilities include:

- Deserialize the event.
- Validate business assumptions.
- Apply side effects safely.
- Store idempotency state.
- Commit offsets at the right time.
- Report failures clearly.

A reliable consumer often follows this shape:

```text
receive event
        |
        v
check idempotency
        |
        v
apply business change
        |
        v
store processed marker
        |
        v
commit offset
```

The idempotency marker and business change should usually be stored atomically when possible.

---

## Retry Design

Not every failure should be retried in the same way.

Retryable failures:

```text
temporary network issue
database timeout
dependent service temporarily unavailable
```

Non-retryable failures:

```text
invalid event structure
unknown required business reference
unsupported event version
permanent validation failure
```

Retries should use backoff.

Immediate tight-loop retries can overload the same dependency that is already failing.

Spring Kafka error handlers can classify exceptions and recover exhausted records to a dead letter topic.

The retry strategy should match the failure type.

---

## Dead Letter Topics

A dead letter topic stores records that could not be processed successfully after the chosen recovery strategy.

```text
main topic
    |
    v
consumer
    |
    X repeated failure
    |
    v
dead letter topic
```

A DLT is not a trash can.

It is an operational signal.

DLT records should be observable and actionable.

For each DLT, define:

- Who owns it
- What alerts on it
- How records are inspected
- Whether records can be replayed
- How fixed records are recovered

Without an operational process, a DLT only hides failures.

---

## Idempotency

Idempotency prevents duplicate delivery from becoming duplicate business effects.

Use an idempotency key that is stable for the logical event.

Examples:

```text
eventId
orderId + eventType + version
paymentId
```

Avoid generating a new idempotency key on every retry.

That makes duplicate detection impossible.

Store idempotency state in the same transaction as the business side effect when possible.

```text
BEGIN TRANSACTION

apply business update
store processed event id

COMMIT
```

---

## Observability

Kafka systems need operational visibility.

Important signals include:

- Consumer lag
- Processing latency
- Retry counts
- DLT counts
- Deserialization failures
- Rebalance frequency
- Commit failures
- Producer send failures
- Schema compatibility failures

Logs should include enough context to investigate a record.

Useful fields:

```text
topic
partition
offset
key
event type
event id
consumer group
delivery attempt
exception type
```

Without this context, debugging Kafka failures becomes slow.

---

## Security and Ownership

Event-driven systems need ownership boundaries.

Each topic should have a clear owner.

The owner is responsible for:

- Event meaning
- Schema evolution
- Compatibility policy
- Retention policy
- Access control
- Documentation
- DLT handling

Consumers should not depend on undocumented fields or behavior.

Producers should not change shared event contracts casually.

---

## Design Checklist

Before introducing a new Kafka event, ask:

```text
What happened?
Who owns this event?
Who consumes it?
What is the message key?
What ordering is required?
What schema compatibility is required?
What is the retention policy?
Can consumers handle duplicates?
What failures are retryable?
Where do exhausted failures go?
How will this be monitored?
How will this event evolve?
```

These questions prevent many production problems.

---

## Common Mistakes

### Designing topics around producer internals

Topics should reflect useful event streams, not just internal service structure.

### Choosing message keys randomly

Keys affect partitioning and ordering.

Choose keys based on processing requirements.

### Publishing events without clear ownership

Shared events need owners.

Without ownership, schema changes and operational issues become unclear.

### Treating the DLT as the solution

A DLT stores failures.

It does not solve them unless there is a process for investigation and recovery.

### Ignoring observability

Kafka failures often happen outside normal application call stacks.

Logs and metrics must include topic, partition, offset, key, and exception context.

### Forgetting idempotency

Retries, rebalances, and replay can all repeat records.

Consumers should be designed for duplicate delivery.

---

## Interview Corner

### How do you choose a Kafka message key?

Choose a key based on ordering and partitioning requirements.

If events for the same entity must be processed in order, use that entity's identifier.

### How do you design a topic?

Group related events that share consumption, retention, ordering, and evolution needs.

Avoid mixing unrelated events just because the same service produces them.

### Why is idempotency important in event-driven systems?

Kafka consumers can see duplicate records due to retries, crashes, rebalances, or replay.

Idempotency prevents duplicates from becoming repeated business side effects.

### What should go into a dead letter topic?

Records that cannot be processed after the configured retry and recovery strategy.

DLT records should be monitored and have an operational recovery process.

### What is the difference between schema compatibility and business compatibility?

Schema compatibility checks whether data can be read across schema versions.

Business compatibility asks whether the event still means what consumers expect it to mean.

---

## Rules of Thumb

- Name events after facts that happened.
- Choose message keys based on ordering requirements.
- Treat schemas as long-lived contracts.
- Keep topic ownership explicit.
- Use compatibility checks for shared schemas.
- Make consumers idempotent.
- Use retries for temporary failures.
- Use DLTs for exhausted or unrecoverable records.
- Monitor lag, retries, DLTs, and deserialization failures.
- Include topic, partition, offset, key, and event ID in logs.
- Design for replay before replay is needed.
