# Delivery Semantics

## Introduction

Delivery semantics describe what can happen to records when failures occur.

In Kafka systems, failures can happen at several points:

```text
Producer
   |
   v
Kafka broker
   |
   v
Consumer
   |
   v
Database / external system
```

A producer can retry.

A broker can fail.

A consumer can crash after processing but before committing its offset.

A database write can succeed while a Kafka commit fails.

Delivery semantics help answer:

> Can a record be lost, duplicated, or processed more than once?

The common terms are:

```text
at-most-once
at-least-once
exactly-once
```

These terms are useful, but they are often misunderstood.

The important question is always:

> Exactly once where?

---

## At-Most-Once

At-most-once means a record is processed zero or one time.

Duplicates are avoided, but loss is possible.

Conceptually:

```text
commit offset
        |
        v
process record
```

If the application crashes after committing the offset but before processing completes, the record may not be delivered again.

```text
offset committed
        |
        X crash before processing
        |
        v
record skipped after restart
```

At-most-once can be acceptable when losing some records is better than processing duplicates.

Examples may include:

- Some metrics
- Some logs
- Non-critical telemetry

It is usually not acceptable for business events such as payments, orders, inventory updates, or account changes.

---

## At-Least-Once

At-least-once means a record should not be lost, but it may be processed more than once.

Conceptually:

```text
process record
        |
        v
commit offset
```

If processing succeeds but the application crashes before committing the offset, the record can be redelivered.

```text
process succeeded
        |
        X crash before offset commit
        |
        v
record delivered again
```

At-least-once is common in backend systems because it favors not losing records.

The trade-off is duplicate processing.

Consumers must be designed to handle duplicates safely.

---

## Duplicate Delivery

Duplicate delivery can happen for several reasons.

Examples:

- Producer retries after a lost acknowledgement.
- A consumer crashes before committing an offset.
- A rebalance happens before offsets are committed.
- A record is replayed after an offset reset.
- A transaction partially succeeds across Kafka and an external system.

Duplicate delivery does not always mean duplicate business effects.

The application can protect itself with idempotency.

```text
Duplicate record
        |
        v
Check idempotency key
        |
        +--> already processed -> skip side effect
        |
        +--> new record -> process
```

This is why delivery semantics and idempotent consumer design belong together.

---

## Producer Idempotence

Kafka producer idempotence prevents duplicates caused by producer retries within a producer session.

Consider:

```text
Producer                           Broker

   | ---- Event A --------------> |
   |                               | store Event A
   | <--------- ACK ------------- X
   |
   | retry Event A -------------> |
```

Without idempotence, the retry could append a duplicate record.

With idempotence, Kafka can detect the retry and avoid appending the same producer sequence again.

Producer idempotence protects Kafka writes from producer retry duplicates.

It does not make the whole business operation idempotent.

For example:

```java
kafkaTemplate.send("orders", event);
kafkaTemplate.send("orders", event);
```

These are two separate application sends.

Producer idempotence does not know whether the business event is logically duplicated.

---

## Kafka Transactions

Kafka transactions allow multiple Kafka operations to commit or abort together.

For example:

```text
BEGIN KAFKA TRANSACTION

send withdraw event
send deposit event
send consumed offsets

COMMIT KAFKA TRANSACTION
```

This is useful for consume-process-produce workflows.

```text
input topic
     |
     v
processor
     |
     v
output topic
```

The processor can produce output records and commit consumed offsets as part of the same Kafka transaction.

Consumers using `read_committed` will not return records from aborted transactions as part of normal consumption.

Kafka transactions improve atomicity inside Kafka.

They do not automatically make external systems such as databases part of one global transaction.

---

## Exactly-Once in Kafka

Exactly-once is the most misunderstood delivery term.

In Kafka, exactly-once semantics are primarily about Kafka's read-process-write pipeline.

They rely on:

- Producer idempotence
- Kafka transactions
- Transactional offset commits
- Consumers configured to read committed data when needed

Conceptually:

```text
consume input record
        |
        v
produce output records
        |
        v
commit consumed offset

All inside one Kafka transaction
```

This prevents a Kafka processor from producing output without committing the corresponding input offset, or committing the input offset without the output records.

However:

> Exactly-once in Kafka does not automatically mean exactly-once business effects in every external system.

If the consumer writes to a database, calls an HTTP API, sends an email, or charges a payment method, those systems have their own failure modes.

Application-level idempotency is still required.

---

## Consumer Idempotency

An idempotent consumer can safely process the same logical message more than once.

Example:

```text
messageId = ORD-123-created
```

The consumer stores the processed message ID with the business operation.

```text
BEGIN DATABASE TRANSACTION

check message_id
apply business change
store message_id

COMMIT
```

If the record is delivered again:

```text
message_id already exists
        |
        v
skip duplicate side effect
```

This protects the business operation from duplicate delivery.

Idempotency is usually the application layer's responsibility.

---

## External Side Effects

Kafka can control Kafka writes.

It cannot fully control every external side effect.

Consider:

```text
consume payment event
        |
        v
call payment provider
        |
        v
commit offset
```

If the provider call succeeds but the consumer crashes before committing the offset, the event may be delivered again.

The payment provider may be called twice unless the application uses an idempotency key.

```text
Payment provider request

Idempotency-Key: payment-123
```

External systems need their own duplicate protection.

Kafka delivery guarantees do not remove that need.

---

## Delivery Semantics and Ordering

Ordering and delivery semantics are related but separate.

Kafka guarantees ordering within a partition.

```text
Partition 0

Offset 0 -> Event A
Offset 1 -> Event B
Offset 2 -> Event C
```

Retries, failures, and reprocessing can still cause the application to observe repeated records.

```text
A
B
B again after retry/restart
C
```

The order within the partition is preserved, but duplicate processing is still possible.

If business ordering matters, use a message key that routes related events to the same partition.

If duplicate processing matters, use idempotency.

They solve different problems.

---

## Trade-Offs

Delivery choices involve trade-offs.

```text
Lower latency
        |
        v
Fewer waits, fewer commits, weaker guarantees

Stronger guarantees
        |
        v
More coordination, more commits, higher latency
```

Examples:

| Goal | Typical Cost |
| --- | --- |
| Avoid duplicates | Idempotency state, transactions, or deduplication logic |
| Avoid loss | Commit after processing, tolerate redelivery |
| Higher throughput | Batch processing, fewer commits |
| Lower latency | Smaller batches, faster commits |
| Stronger Kafka atomicity | Transactions and more coordination |

There is no universally best setting.

The correct design depends on the business consequence of loss, duplicates, delay, and reprocessing.

---

## Common Mistakes

### Assuming at-least-once means exactly once

At-least-once protects against loss, but duplicates are possible.

Consumers still need idempotency.

### Assuming producer idempotence prevents business duplicates

Producer idempotence protects against duplicates caused by producer retries.

It does not know whether two application sends represent the same business event.

### Assuming Kafka transactions include the database

Kafka transactions coordinate Kafka operations.

Database transactions require database transaction management and careful cross-resource design.

### Treating exactly-once as magic

Exactly-once has a specific scope.

It does not automatically make HTTP calls, emails, payment requests, or database writes exactly once.

### Committing offsets before side effects are safe

Committing too early can cause records to be skipped after a crash.

### Ignoring duplicate handling during replay

Offset resets and reprocessing can repeat old records.

Consumers should be safe to replay before offsets are reset.

---

## Interview Corner

### What is at-most-once delivery?

At-most-once means a record is processed zero or one time.

Duplicates are avoided, but loss is possible.

### What is at-least-once delivery?

At-least-once means a record should not be lost, but it may be processed more than once.

Consumers need idempotency to avoid duplicate business effects.

### What does producer idempotence solve?

It prevents duplicate Kafka appends caused by producer retries.

It does not prevent an application from intentionally or accidentally sending the same business event twice.

### What do Kafka transactions solve?

Kafka transactions allow multiple Kafka operations to commit or abort atomically.

They are especially useful for consume-process-produce workflows.

### Does exactly-once mean no duplicate business effects?

Not necessarily.

Exactly-once must be understood within its scope.

Kafka can provide exactly-once processing for Kafka read-process-write flows, but external side effects still require application-level idempotency.

---

## Rules of Thumb

- Use at-least-once for most important business events.
- Make consumers idempotent.
- Commit offsets after processing is safe.
- Use producer idempotence to protect against retry duplicates.
- Use Kafka transactions for atomic Kafka read-process-write workflows.
- Use `read_committed` when consumers should ignore aborted transactional records.
- Do not assume Kafka transactions include external systems.
- Use idempotency keys for external APIs.
- Treat replay as duplicate delivery.
- Always define what "exactly once" means in the specific system boundary being discussed.
