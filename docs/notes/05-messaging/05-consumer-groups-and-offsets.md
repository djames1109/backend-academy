# Consumer Groups and Offsets

## Introduction

Kafka consumers usually run as part of a consumer group.

A consumer group allows multiple consumer instances to cooperate when processing records from one or more topics.

Kafka assigns partitions to consumers in the group.

Each partition is consumed by only one consumer in the same group at a time.

```text
Topic: order-created

Partition 0  ──> Consumer A
Partition 1  ──> Consumer B
Partition 2  ──> Consumer C

Consumer Group: inventory-service
```

Consumer groups provide the foundation for parallel processing, rebalancing, and offset tracking.

Offsets define where a consumer group is in each partition.

Understanding offsets is essential because offset commits determine what Kafka considers already processed by a group.

---

## Consumer Groups

A consumer group is identified by a `group.id`.

```yaml
spring:
  kafka:
    consumer:
      group-id: inventory-service
```

All consumer instances with the same `group.id` cooperate as one logical application.

```text
inventory-service instance 1
inventory-service instance 2
inventory-service instance 3
        |
        v
Consumer Group: inventory-service
```

Kafka distributes topic partitions across the consumers in the group.

This allows the application to scale processing across multiple instances.

---

## Partition Assignment

Within a consumer group, a partition is assigned to one consumer at a time.

```text
Topic partitions:

P0
P1
P2

Consumer group:

Consumer A -> P0
Consumer B -> P1
Consumer C -> P2
```

If there are more consumers than partitions, some consumers are idle.

```text
3 partitions
5 consumers

Consumer A -> P0
Consumer B -> P1
Consumer C -> P2
Consumer D -> idle
Consumer E -> idle
```

Adding consumers beyond the number of partitions does not increase processing parallelism within that group.

The number of partitions places an upper bound on useful consumer parallelism.

---

## Multiple Consumer Groups

Different consumer groups read the same topic independently.

```text
Topic: order-created

        +--> Group: inventory-service
        |
        +--> Group: notification-service
        |
        +--> Group: analytics-service
```

Each group maintains its own offsets.

This allows multiple services to process the same events for different purposes.

```text
inventory-service offset      -> P0 offset 200
notification-service offset   -> P0 offset 185
analytics-service offset      -> P0 offset 500
```

The groups do not block each other.

They move through the topic independently.

---

## Offsets

Every record in a partition has an offset.

```text
Partition 0

Offset 0 -> Event A
Offset 1 -> Event B
Offset 2 -> Event C
Offset 3 -> Event D
```

An offset is only unique within a partition.

The full position of a record is:

```text
topic + partition + offset
```

Consumers read from a position in each assigned partition.

```text
Consumer position
        |
        v
Offset 2 -> Event C
Offset 3 -> Event D
Offset 4 -> Event E
```

The position can move as the consumer polls records.

The committed offset is the position stored for recovery.

---

## Committed Offsets

A committed offset tells Kafka where a consumer group should resume after a restart or rebalance.

For example:

```text
Committed offset for group inventory-service

order-created / partition 0 -> offset 4
```

This means the group has committed progress through the records before that next position.

If the consumer restarts, another consumer in the group can resume from the committed position.

```text
Consumer crashes
        |
        v
Partition reassigned
        |
        v
Resume from committed offset
```

Offset commits are therefore part of the application's delivery semantics.

Committing too early can lose records.

Committing too late can cause duplicates.

---

## Auto Offset Reset

`auto.offset.reset` controls where a consumer starts when there is no committed offset for the group or when the committed offset is no longer valid.

Common values are:

```text
earliest
latest
none
```

`earliest` means start from the earliest available offset.

```yaml
spring:
  kafka:
    consumer:
      auto-offset-reset: earliest
```

This is useful when a new group should process existing records.

`latest` means start from new records produced after the consumer starts.

```yaml
spring:
  kafka:
    consumer:
      auto-offset-reset: latest
```

This is useful when only new events matter.

`none` means fail if no valid committed offset exists.

This is useful when starting at an unexpected position would be dangerous.

Important point:

> `auto.offset.reset` is not used every time a consumer starts. It is used when Kafka has no valid committed offset for that group and partition.

---

## Auto Commit

Kafka consumers can automatically commit offsets.

```yaml
spring:
  kafka:
    consumer:
      enable-auto-commit: true
```

With auto commit, the consumer periodically commits offsets based on configuration.

This is convenient, but it can make processing guarantees harder to reason about.

Consider:

```text
poll records
        |
        v
offset auto-committed
        |
        v
application crashes before processing completes
```

After restart, the consumer may resume after the committed offset.

The record may not be processed again.

That can cause message loss from the application's point of view.

For reliable processing, Spring Kafka applications commonly disable Kafka auto commit and let the listener container manage commits.

---

## Spring Kafka Ack Modes

Spring Kafka listener containers can manage offset commits with acknowledgment modes.

Common modes include:

```text
RECORD
BATCH
MANUAL
MANUAL_IMMEDIATE
```

`RECORD` commits after each record is processed.

```text
process record
        |
        v
commit offset
```

`BATCH` commits after all records from the poll are processed.

```text
poll batch
        |
        v
process all records
        |
        v
commit offsets
```

`MANUAL` allows listener code to acknowledge processing.

```java
@KafkaListener(topics = "order-created")
public void consume(
    OrderCreatedEvent event,
    Acknowledgment acknowledgment
) {
    process(event);

    acknowledgment.acknowledge();
}
```

Manual acknowledgment gives the application more control over when progress is committed.

It also creates more responsibility.

If acknowledgment happens before the business operation is safely completed, records may be skipped after a crash.

---

## At-Least-Once Processing

A common reliable-consumer pattern is:

```text
poll record
        |
        v
process business operation
        |
        v
commit offset
```

If the application crashes after processing but before committing the offset, Kafka may deliver the record again.

```text
process succeeded
        |
        X crash before offset commit
        |
        v
record redelivered
```

This is at-least-once processing.

The record is not lost, but duplicate processing is possible.

For this reason, consumers should be idempotent when duplicate delivery can happen.

---

## Rebalancing

A rebalance happens when partition ownership in a consumer group changes.

Common causes include:

- A consumer starts.
- A consumer stops.
- A consumer becomes unhealthy.
- Topic partitions change.
- Subscription changes.

Conceptually:

```text
Before rebalance

Consumer A -> P0, P1
Consumer B -> P2

After Consumer C joins

Consumer A -> P0
Consumer B -> P1
Consumer C -> P2
```

During a rebalance, partitions can move between consumers.

This affects offset commits because a consumer should not continue committing offsets for partitions it no longer owns.

The application should expect that rebalances can happen during normal operation.

---

## Consumer Lag

Consumer lag is the distance between the latest available offset and the consumer group's committed or processed position.

```text
Latest offset:    1000
Committed offset:  850

Lag: 150
```

Lag indicates how far behind a consumer group is.

High lag can mean:

- Consumers are too slow.
- There are too few partitions or consumers.
- Downstream systems are slow.
- Error handling is blocking progress.
- A consumer is unhealthy.

Lag should be monitored per topic, partition, and consumer group.

Lag is not always bad.

Batch workloads and replay jobs may intentionally build and drain lag.

The important question is whether lag is expected and under control.

---

## Offset Reset and Replay

Kafka allows consumers to reprocess data by changing offsets.

Examples:

```text
Reset group to earliest
Reset group to a timestamp
Reset group to a specific offset
```

Replay can be useful for:

- Rebuilding read models
- Recovering from a bad deployment
- Reprocessing after a bug fix
- Backfilling a new downstream system

Replay is powerful, but it is dangerous if consumers are not idempotent.

Reprocessing old events can repeat side effects.

Before resetting offsets, understand what the consumer does with each event.

---

## Common Mistakes

### Thinking offsets belong to consumers instead of groups

Offsets are tracked for a consumer group.

Different groups can have different offsets for the same topic and partition.

### Assuming auto.offset.reset controls every restart

`auto.offset.reset` is only used when there is no valid committed offset for the group and partition.

If a committed offset exists, Kafka resumes from that committed position.

### Committing before processing is safe

If the offset is committed before the business operation completes, a crash can cause the record to be skipped.

### Adding more consumers than partitions

Extra consumers in the same group remain idle when there are no partitions left to assign.

### Ignoring rebalances

Rebalances are normal in Kafka.

Consumers should be designed with partition movement and redelivery in mind.

### Resetting offsets without idempotency

Replay can repeat side effects.

Consumers should be safe to reprocess records before offsets are reset.

---

## Interview Corner

### What is a consumer group?

A consumer group is a set of consumers that cooperate to process topic partitions.

Each partition is assigned to one consumer in the group at a time.

### What is a committed offset?

A committed offset is the stored progress for a consumer group on a topic partition.

It determines where the group resumes after a restart or rebalance.

### What does auto.offset.reset do?

It controls where consumption starts when no valid committed offset exists.

Common values are `earliest`, `latest`, and `none`.

### Why can at-least-once consumers process duplicates?

If processing succeeds but the offset commit fails or the application crashes before committing, the same record can be delivered again.

### Why does partition count affect consumer parallelism?

Within one consumer group, each partition can be actively consumed by only one consumer at a time.

The number of partitions limits how many consumers can actively process the topic in parallel.

---

## Rules of Thumb

- Treat offsets as consumer-group progress.
- Use a stable `group.id` for each logical application.
- Use separate consumer groups for independent services.
- Understand when `auto.offset.reset` applies.
- Prefer committing offsets after processing succeeds.
- Expect duplicate delivery when using at-least-once processing.
- Make consumers idempotent before relying on replay.
- Monitor lag per group, topic, and partition.
- Do not add more consumers than partitions solely for parallelism.
- Treat offset resets as operational changes that require care.
