# Kafka Producer

## Introduction

A Kafka producer is responsible for publishing records to Kafka topics.

In Spring Boot applications, Spring Kafka provides `KafkaTemplate` as a high-level abstraction for publishing records using the Kafka producer client.

Publishing a record may appear to be a simple operation, but several decisions affect how reliably and efficiently records are delivered:

- Should the producer wait for acknowledgement from Kafka?
- How many replicas must receive the record?
- What happens when a temporary failure occurs?
- How long should the producer keep retrying?
- How can duplicate records caused by retries be prevented?
- How can multiple requests be sent efficiently while preserving ordering?

Understanding these behaviors is important when building reliable event-driven applications.

---

## Publishing with KafkaTemplate

Spring Kafka provides `KafkaTemplate` for publishing records.

A record can contain both a key and a value.

```java
kafkaTemplate.send(
    "product-created",
    event.productId(),
    event
);
```

In this example:

- `product-created` is the topic.
- `event.productId()` is the record key.
- `event` is the record value.

The key can influence which partition receives the record.

Records with the same key are normally routed to the same partition, assuming the number of partitions does not change.

---

## Asynchronous Publishing

`KafkaTemplate.send()` is asynchronous and returns a `CompletableFuture`.

This allows the calling thread to continue without waiting for Kafka to acknowledge the record.

```java
var future = kafkaTemplate.send(
    "product-created",
    event.productId(),
    event
);

future.whenComplete((result, throwable) -> {
    if (throwable != null) {
        // handle failure
    } else {
        // publish completed successfully
    }
});
```

The result contains metadata about the published record.

For example:

```java
var metadata = result.getRecordMetadata();

metadata.topic();
metadata.partition();
metadata.offset();
```

This identifies where the record was stored.

```text
Topic:     product-created
Partition: 1
Offset:    42
```

---

## Synchronous Publishing

The same asynchronous API can be made effectively synchronous by waiting for the returned future.

```java
SendResult<String, ProductCreatedEvent> result =
    kafkaTemplate.send(
        "product-created",
        event.productId(),
        event
    ).get();
```

Calling `.get()` blocks the calling thread until the send operation completes or fails.

For example, if this happens while processing an HTTP request:

```text
HTTP Request
     |
     v
Request Thread
     |
     | KafkaTemplate.send(...).get()
     |
     | waiting...
     |
     v
Kafka acknowledgement
     |
     v
Continue processing
```

The request-handling thread remains blocked while waiting for Kafka.

Asynchronous publishing avoids this blocking behavior when the application does not need the result before continuing.

> A successful producer acknowledgement means Kafka accepted the record according to the configured acknowledgement policy. It does not mean that a consumer has already processed the record.

---

# Producer Acknowledgements

The `acks` producer configuration determines what acknowledgement is required before a publish is considered successful.

The main options are:

```text
acks=0
acks=1
acks=all
```

These provide different trade-offs between latency and durability.

---

## acks=0

With:

```yaml
spring:
  kafka:
    producer:
      acks: 0
```

the producer does not wait for acknowledgement from the broker.

Conceptually:

```text
Producer --------------------> Broker
         record

Producer continues immediately
```

This provides the weakest delivery guarantee because the producer cannot rely on a broker acknowledgement confirming that the record was stored.

---

## acks=1

With:

```yaml
spring:
  kafka:
    producer:
      acks: 1
```

the partition leader must accept the record before the producer receives a successful acknowledgement.

```text
Producer                 Leader                 Follower

   | ---- Record -------> |
   |                      | store
   | <------ ACK -------- |
   |                      |
   |                      | ---- replicate ----> |
```

The leader does not need to wait for followers to replicate the record before acknowledging it.

This means there is a possible failure window.

If the leader acknowledges the record and fails before a follower replicates it, the acknowledged record may be lost.

---

## acks=all

With:

```yaml
spring:
  kafka:
    producer:
      acks: all
```

the producer requests the strongest acknowledgement behavior.

The leader waits for the required in-sync replicas before acknowledging the write.

This configuration works together with:

```text
min.insync.replicas
```

which is configured on the Kafka topic or broker side.

---

# min.insync.replicas

`min.insync.replicas` defines the minimum number of in-sync replicas required for an `acks=all` write to succeed.

For example:

```text
replication.factor = 3
min.insync.replicas = 2
```

Suppose all three replicas are available:

```text
Broker 1 → Leader       ISR
Broker 2 → Follower     ISR
Broker 3 → Follower     ISR
```

An `acks=all` write can succeed.

If one broker becomes unavailable:

```text
Broker 1 → Leader       ISR
Broker 2 → Follower     ISR
Broker 3 → Down
```

there are still two in-sync replicas.

The write can still succeed.

However, if another replica becomes unavailable:

```text
Broker 1 → Leader       ISR
Broker 2 → Down
Broker 3 → Down
```

then:

```text
Current ISR = 1
min.insync.replicas = 2
```

Kafka cannot satisfy the durability requirement, so the `acks=all` write cannot successfully complete while this condition remains.

This creates an important trade-off:

```text
Stronger durability
        ↑
        |
acks=all + min.insync.replicas
        |
        ↓
Potentially lower availability during broker failures
```

`min.insync.replicas` does not provide the same protection when the producer uses `acks=0` or `acks=1`.

---

# Producer Retries

Publishing can fail because of temporary problems.

Examples include:

- Temporary network failures
- Broker unavailability
- Leader changes
- Leader elections
- Insufficient in-sync replicas

Some failures are **retriable**, meaning the problem may disappear if the operation is attempted again.

Other failures are **non-retriable**.

For example, if a record exceeds the configured maximum message size, repeatedly sending the same record will not solve the problem.

Kafka Producer determines whether an error is retriable and automatically handles retries for retriable producer failures.

The application does not normally need to implement this retry loop itself.

---

## retries

The `retries` property limits how many retries Kafka Producer can perform for retriable failures.

```yaml
spring:
  kafka:
    producer:
      retries: 10
```

Conceptually:

```text
Send
 |
 X temporary failure
 |
Retry
 |
 X temporary failure
 |
Retry
 |
 ✓ success
```

Modern Kafka uses a very large default value for `retries`.

Rather than primarily controlling delivery through a small retry count, Kafka generally recommends using `delivery.timeout.ms` to bound how long delivery may continue.

---

## retry.backoff.ms

The producer does not need to retry immediately after a retriable failure.

`retry.backoff.ms` controls the base delay associated with retrying requests.

```yaml
spring:
  kafka:
    producer:
      properties:
        retry.backoff.ms: 1000
```

Conceptually:

```text
Send
 |
 X failure
 |
Wait / backoff
 |
Retry
```

Backoff prevents the producer from repeatedly sending requests in a tight loop while a temporary problem is occurring.

---

# Producer Timeouts

Several timeout-related properties control different parts of record delivery.

Two important properties are:

```text
request.timeout.ms
delivery.timeout.ms
```

They represent different scopes.

---

## request.timeout.ms

`request.timeout.ms` controls how long the producer waits for a response to an individual broker request.

For example:

```yaml
spring:
  kafka:
    producer:
      properties:
        request.timeout.ms: 30000
```

Conceptually:

```text
Producer ------------------------> Broker
           request

         waiting...
         waiting...

       request timeout
```

A request timing out does not necessarily mean the entire record delivery process is immediately over.

If the failure is retriable and the overall delivery deadline has not been reached, Kafka may retry.

---

## delivery.timeout.ms

`delivery.timeout.ms` places an overall bound on how long the producer may spend trying to deliver a record.

```yaml
spring:
  kafka:
    producer:
      properties:
        delivery.timeout.ms: 120000
```

It covers the overall delivery process, including time spent:

- Waiting before sending
- Sending requests
- Waiting for broker responses
- Backing off
- Retrying

Conceptually:

```text
|------------------- delivery.timeout.ms -------------------|

      request          backoff          request
     |-------|        |-------|        |-------|
         X                                ✓
      failure                           success
```

This is different from `request.timeout.ms`, which applies to an individual request.

A useful mental model is:

```text
request.timeout.ms
    → How long can this broker request wait?

delivery.timeout.ms
    → How long can delivery of this record continue overall?
```

The producer may stop before the delivery timeout if another stopping condition is reached first, such as a non-retriable error or an explicitly configured retry limit.

---

# retries vs delivery.timeout.ms

`retries` and `delivery.timeout.ms` do not conflict.

They place different limits on the producer's retry behavior.

For example:

```text
retries = 10
delivery.timeout.ms = 120000
```

The producer stops retrying when the applicable limit is reached.

Conceptually:

```text
                 delivery.timeout.ms
       <---------------------------------->

Attempt 1
    |
    X
    |
backoff
    |
Attempt 2
    |
    X
    |
backoff
    |
Attempt 3
    |
    ✓
```

If the configured retries are exhausted before the delivery timeout, the delivery can fail earlier.

If a very large retry count is configured, the delivery timeout can become the practical upper bound.

For this reason, modern Kafka commonly uses a large retry count while controlling the overall retry window through `delivery.timeout.ms`.

---

# Batching and linger.ms

Kafka Producer can batch multiple records together before sending them to a broker.

For example:

```text
Application

send(A)
send(B)
send(C)

    |
    v

Kafka Producer

A ─┐
B ─┼──> Batch ───> Broker
C ─┘
```

Batching reduces the number of network requests and can improve throughput.

`linger.ms` controls how long the producer may wait for additional records before sending a batch.

```yaml
spring:
  kafka:
    producer:
      properties:
        linger.ms: 5
```

Increasing `linger.ms` can allow more records to accumulate in a batch.

This introduces a trade-off:

```text
Higher linger
    |
    +--> potentially larger batches
    +--> potentially better throughput
    +--> potentially better compression
    |
    +--> potentially higher latency
```

---

# Producer Idempotence

Retries introduce an important problem.

Consider the following scenario:

```text
Producer                           Broker

   | ---- Event A --------------> |
   |                               | Store A
   |                               |
   | <--------- ACK ------------- X
   |
   | acknowledgement was lost
```

The producer cannot safely assume that the broker did not receive the record.

If it retries:

```text
Producer                           Broker

   | ---- Event A --------------> |
```

without duplicate protection, the same logical producer attempt could result in another record being appended.

Producer idempotence is designed to prevent duplicates caused by this kind of producer retry.

It can be explicitly enabled using:

```yaml
spring:
  kafka:
    producer:
      properties:
        enable.idempotence: true
```

Kafka enables producer idempotence by default when the producer configuration is compatible with it.

Explicitly configuring it can make the intended behavior clear.

---

## Idempotence Requirements

Producer idempotence requires compatible producer settings.

The important requirements are:

```text
acks = all

retries > 0

max.in.flight.requests.per.connection <= 5
```

Conceptually:

```yaml
spring:
  kafka:
    producer:
      acks: all
      properties:
        enable.idempotence: true
        max.in.flight.requests.per.connection: 5
```

If idempotence is explicitly enabled while incompatible properties are explicitly configured, the producer configuration can fail.

---

# How Producer Idempotence Works

Kafka producer idempotence does not inspect the contents of an event to determine whether it is a duplicate.

It does not perform checks such as:

```text
eventId == previousEventId
```

Instead, Kafka uses protocol-level information between the producer and broker.

Kafka uses concepts including:

- Producer ID
- Sequence numbers

Conceptually:

```text
Producer ID = 123

Partition 0

Event A → sequence 0
Event B → sequence 1
Event C → sequence 2
```

The producer includes this information when sending batches to Kafka.

The broker tracks producer sequence information and can use it to identify duplicate retries and preserve ordering.

This mechanism is handled internally by the Kafka producer client and broker.

Applications do not manually assign Kafka producer sequence numbers.

---

# Producer Idempotence vs Application Idempotency

Kafka producer idempotence should not be confused with application-level or business-level idempotency.

Producer idempotence protects against duplicate records caused by **Kafka Producer's internal retry mechanism**.

For example:

```text
Application

send(A)
   |
   v
Kafka Producer

attempt #1 ------> failure / uncertain acknowledgement

attempt #2 ------> retry

attempt #3 ------> retry
```

The application called `send()` once.

Kafka Producer performed multiple attempts internally.

Producer idempotence protects this scenario.

However, consider:

```java
kafkaTemplate.send(topic, event);

// application later decides to publish again

kafkaTemplate.send(topic, event);
```

These are separate application-level send operations.

Producer idempotence should not be treated as business-level deduplication for these separate sends.

Even if both records contain:

```text
eventId = 12345
```

Kafka producer idempotence is not checking the business identifier and deciding whether the event has already been published.

Application-level duplicate prevention requires a separate idempotency strategy.

---

# In-Flight Requests

Kafka Producer can send multiple network requests to a broker without waiting for every previous request to receive a response.

A request that has been sent but has not yet received its response is considered **in flight**.

For example:

```text
Producer                              Broker

Request A --------------------------->
Request B --------------------------->
Request C --------------------------->
Request D --------------------------->
Request E --------------------------->

          <--------------------------- Response A
          <--------------------------- Response B
          <--------------------------- Response C
```

These are separate requests.

They are not five retries of the same request.

The maximum number of requests that may be outstanding on a broker connection is controlled by:

```text
max.in.flight.requests.per.connection
```

For example:

```yaml
spring:
  kafka:
    producer:
      properties:
        max.in.flight.requests.per.connection: 5
```

This means that up to five requests may be waiting for responses on a connection at the same time.

---

## send() vs Produce Request

An application call to:

```java
kafkaTemplate.send(...)
```

should not be thought of as being exactly equivalent to one Kafka network request.

Kafka Producer buffers records and groups them into batches.

For example:

```text
Application

send(A)
send(B)
send(C)
send(D)

       |
       v

Kafka Producer

A + B ───> Produce Request #1
C + D ───> Produce Request #2

       |
       v

Broker
```

Kafka Producer also maintains and reuses connections to Kafka brokers.

Calling `send()` multiple times does not normally create a new TCP connection for every record.

---

# Why In-Flight Requests Matter for Idempotence

At first, in-flight requests and producer idempotence may appear unrelated.

Idempotence protects against retries, while in-flight requests are separate outstanding requests.

They become related when an earlier request needs to be retried while later requests are already in flight.

For example:

```text
Producer                              Broker

Request A (seq 0) ------------------->
Request B (seq 1) ------------------->
Request C (seq 2) ------------------->
```

Suppose the broker stores A but its response is lost.

```text
Broker

seq 0 → A ✓
seq 1 → B ✓
seq 2 → C ✓
```

The producer may need to retry A because it does not know that A was successfully stored.

```text
Retry A (seq 0) -------------------->
```

Because the broker tracks the producer ID and sequence numbers, it can recognize that sequence `0` was already accepted and avoid appending A again.

The sequence protocol also helps Kafka preserve ordering when several requests are outstanding while retries occur.

Kafka therefore limits:

```text
max.in.flight.requests.per.connection <= 5
```

when producer idempotence is enabled.

This allows Kafka to maintain the required sequence and duplicate-detection state while still permitting multiple requests to be processed concurrently.

---

# Producer Configuration vs Topic Configuration

Producer and topic configurations belong to different parts of Kafka.

## Producer Configuration

These properties control how the application publishes records.

```yaml
spring:
  kafka:
    producer:
      acks: all
      retries: 10
      properties:
        enable.idempotence: true
        max.in.flight.requests.per.connection: 5
        retry.backoff.ms: 1000
        delivery.timeout.ms: 120000
        request.timeout.ms: 30000
        linger.ms: 5
```

Examples include:

| Property | Purpose |
| --- | --- |
| `acks` | Required acknowledgement level |
| `retries` | Retry limit for retriable failures |
| `retry.backoff.ms` | Delay/base delay associated with retries |
| `delivery.timeout.ms` | Overall delivery deadline |
| `request.timeout.ms` | Timeout for an individual broker request |
| `linger.ms` | Delay used to allow records to form larger batches |
| `enable.idempotence` | Enables duplicate protection for producer retries |
| `max.in.flight.requests.per.connection` | Maximum outstanding requests per broker connection |

## Topic Configuration

These properties or settings belong to the Kafka topic.

```text
partitions = 3
replication.factor = 3
min.insync.replicas = 2
```

| Setting | Purpose |
| --- | --- |
| `partitions` | Number of logical partition logs |
| `replication.factor` | Number of replicas for each partition |
| `min.insync.replicas` | Minimum ISR required for successful `acks=all` writes |

The producer and topic configurations work together.

For example:

```text
Producer                          Topic

acks=all -----------------------> min.insync.replicas=2

                                  replication.factor=3
```

The producer determines what acknowledgement it requires, while the topic determines the replication conditions under which that acknowledgement can be satisfied.

---

# Putting It Together

The producer lifecycle can be viewed as a pipeline:

```text
Application
    |
    | KafkaTemplate.send()
    v
Kafka Producer
    |
    | buffer records
    | batch records
    | linger.ms
    v
Produce Request
    |
    | multiple requests may be in flight
    v
Kafka Broker
    |
    | acknowledgement according to acks
    v
Success
```

If a temporary failure occurs:

```text
Produce Request
    |
    X retriable failure
    |
    v
retry.backoff.ms
    |
    v
Retry
    |
    | producer idempotence prevents
    | duplicate records caused by retries
    v
Kafka Broker
```

The entire delivery process is bounded by:

```text
delivery.timeout.ms
```

while individual broker requests are bounded by:

```text
request.timeout.ms
```

Together, these configurations allow applications to balance:

```text
Durability
Availability
Latency
Throughput
```

depending on the requirements of the system.

---

# Common Mistakes

## Assuming a successful publish means the event was consumed

Producer acknowledgement only confirms the producer-side Kafka write according to the configured `acks` behavior.

It does not mean that a consumer has processed the event.

---

## Assuming acks=1 means the record is fully replicated

`acks=1` only requires the partition leader to acknowledge the record.

Followers may not have replicated the record when the acknowledgement is returned.

---

## Treating min.insync.replicas as a producer property

`min.insync.replicas` belongs to the Kafka topic or broker configuration.

It works together with the producer's `acks=all` configuration.

---

## Implementing immediate application retries for every producer failure

Kafka Producer already handles retriable producer failures internally.

Adding another retry mechanism around `send()` without understanding the producer's retry behavior can introduce additional complexity and duplicate-delivery scenarios.

---

## Assuming producer idempotence provides business idempotency

Producer idempotence protects against duplicates caused by Kafka Producer's internal retries.

It does not prevent an application from explicitly publishing the same business event multiple times.

---

## Assuming every send() creates a new broker connection

Kafka Producer maintains and reuses broker connections.

Multiple `send()` calls do not normally create separate TCP connections.

---

## Assuming one send() equals one network request

Kafka Producer buffers and batches records.

Multiple application-level `send()` calls may therefore be included in a smaller number of Kafka produce requests.

---

# Interview Corner

## What is the difference between acks=0, acks=1, and acks=all?

`acks=0` does not wait for broker acknowledgement.

`acks=1` requires acknowledgement from the partition leader.

`acks=all` provides the strongest acknowledgement behavior and works with `min.insync.replicas` to require sufficient in-sync replication.

---

## What is the difference between request.timeout.ms and delivery.timeout.ms?

`request.timeout.ms` limits how long an individual broker request waits for a response.

`delivery.timeout.ms` limits the overall amount of time allowed to deliver a record, including retries and related delays.

---

## Why does Kafka Producer retry messages?

Temporary failures such as network problems, broker failures, or leader changes may resolve without application intervention.

Kafka Producer can automatically retry these retriable failures.

---

## What problem does producer idempotence solve?

Producer idempotence prevents Kafka Producer's internal retries from causing duplicate records.

It uses producer IDs and sequence numbers rather than inspecting application-level event identifiers.

---

## Does producer idempotence prevent an application from publishing the same event twice?

No.

Two explicit calls to `send()` represent separate application-level send operations.

Preventing duplicate business operations requires application-level idempotency.

---

## What is an in-flight request?

An in-flight request is a request that has been sent to a Kafka broker but has not yet received its response.

Kafka Producer can have multiple requests in flight simultaneously to improve throughput.

---

## Why does max.in.flight.requests.per.connection matter for idempotence?

An earlier request may need to be retried while later requests are already in flight.

Kafka uses producer IDs and sequence numbers to detect duplicate retries and preserve ordering.

Limiting the number of outstanding requests allows Kafka to maintain these guarantees while still supporting concurrent requests.

---

# Rules of Thumb

- Prefer asynchronous publishing when the application does not need to wait for the Kafka result.
- Use synchronous publishing only when the application must know the result before continuing.
- Use `acks=all` when stronger durability is required.
- Configure `min.insync.replicas` together with an appropriate replication factor.
- Let Kafka Producer handle transient producer retries.
- Prefer bounding retry behavior through `delivery.timeout.ms` rather than relying only on a small retry count.
- Understand the difference between request timeout and overall delivery timeout.
- Keep producer idempotence enabled unless there is a specific reason to disable it.
- Do not confuse Kafka producer idempotence with application-level idempotency.
- Remember that `send()` calls, producer batches, network requests, and broker connections are different concepts.