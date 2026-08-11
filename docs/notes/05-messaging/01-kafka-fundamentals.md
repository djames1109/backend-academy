# Kafka Fundamentals

## Introduction

Apache Kafka is a distributed event streaming platform commonly used to exchange events between applications.

Instead of requiring one service to directly call another service and wait for a response, a producer can publish an event to Kafka and allow other applications to consume it independently.

Kafka is designed to handle large volumes of events while providing scalability, durability, ordering, and fault tolerance.

Understanding Kafka starts with a few core concepts: brokers, topics, partitions, offsets, replication, and message keys.

---

## Kafka Cluster

A Kafka cluster consists of one or more Kafka brokers working together.

A **broker** is a Kafka server responsible for storing topic partitions and handling requests from producers and consumers.

For example, a Kafka cluster may contain three brokers:

```text
Kafka Cluster

├── Broker 1
├── Broker 2
└── Broker 3
```

Running multiple brokers allows Kafka to distribute data across different machines and maintain replicas for fault tolerance.

---

## Topics

A **topic** is a logical stream of records.

Producers publish records to topics, while consumers read records from topics.

For example:

```text
Producer
    |
    | ProductCreated
    v
product-created
    |
    v
Consumer
```

A topic does not necessarily exist on only one broker.

Kafka divides topics into **partitions**, and those partitions can be distributed across brokers in the cluster.

---

## Partitions

A topic is divided into one or more partitions.

Each partition is an ordered, append-only log of records.

For example, a topic with three partitions may look like:

```text
product-created

Partition 0
Partition 1
Partition 2
```

When records are published, Kafka determines which partition should contain each record.

Partitions allow Kafka to distribute data and processing across multiple brokers and consumers.

They are one of the main mechanisms Kafka uses to achieve scalability and parallelism.

---

## Offsets

Every record stored in a partition receives an **offset**.

An offset represents the record's position within that partition.

```text
Partition 0

Offset 0 → Event A
Offset 1 → Event B
Offset 2 → Event C
Offset 3 → Event D
```

Offsets are unique only within a partition.

This means another partition can have the same offsets:

```text
Partition 0          Partition 1

0 → Event A          0 → Event X
1 → Event B          1 → Event Y
2 → Event C          2 → Event Z
```

An offset should therefore be understood together with its topic and partition.

Conceptually, a record can be located using:

```text
Topic + Partition + Offset
```

---

## Ordering

Kafka guarantees ordering **within a partition**.

For example:

```text
Partition 0

0 → OrderCreated
1 → PaymentProcessed
2 → OrderCompleted
```

Consumers reading that partition observe the records in the order in which they were appended.

Kafka does not provide a single global ordering across all partitions of a topic.

For example:

```text
Partition 0             Partition 1

0 → Event A             0 → Event B
1 → Event C             1 → Event D
```

There is no global ordering relationship between `Event A` and `Event B`.

This is an important trade-off because multiple partitions allow Kafka to process records in parallel.

---

## Message Keys

A Kafka record can contain a **key**.

```text
Key:   customer-123
Value: CustomerUpdated
```

The key can be used by Kafka to determine which partition receives the record.

Records with the same key are normally routed to the same partition, assuming the topic's partition count does not change.

For example:

```text
customer-123 → Partition 1
customer-456 → Partition 0
customer-123 → Partition 1
customer-789 → Partition 2
customer-123 → Partition 1
```

This is useful when related events must preserve ordering.

For example, events belonging to the same account could use the account ID as their key:

```text
account-123 → AccountCreated
account-123 → MoneyDeposited
account-123 → MoneyWithdrawn
```

Because the same key is routed to the same partition, the ordering of events for that key can be preserved.

---

## Replication

Partitions can be replicated across multiple brokers for fault tolerance.

The number of copies is controlled by the topic's **replication factor**.

For example:

```text
replication.factor = 3
```

means each partition has three replicas.

Suppose a topic has one partition:

```text
Partition 0

Broker 1 → Replica
Broker 2 → Replica
Broker 3 → Replica
```

These are not three different partitions.

They are three copies of the **same partition**.

---

## Leader and Followers

For each partition, one replica acts as the **leader**.

The remaining replicas act as **followers**.

For example:

```text
Partition 0

Broker 1 → Leader
Broker 2 → Follower
Broker 3 → Follower
```

Producers send records to the partition leader.

Followers replicate the partition's data from the leader.

If the leader becomes unavailable, Kafka can elect an eligible replica to become the new leader.

```text
Before failure

Broker 1 → Leader
Broker 2 → Follower
Broker 3 → Follower

Broker 1 fails

Broker 1 → Down
Broker 2 → New Leader
Broker 3 → Follower
```

Replication allows the partition to remain available even when a broker fails.

---

## Replicas and Offsets

Replication does not create additional offsets.

Replicas contain copies of the same partition log.

For example:

```text
Partition 0

Leader          Follower        Follower

0 → Event A     0 → Event A     0 → Event A
1 → Event B     1 → Event B     1 → Event B
2 → Event C     2 → Event C     2 → Event C
```

`Event A` still has offset `0`.

It simply exists physically on multiple brokers because the partition is replicated.

This distinction is useful when separating the responsibilities of partitions and replicas:

- **Partitions** provide separate logs and enable parallelism.
- **Replicas** provide redundancy and fault tolerance.
- **Offsets** identify positions within a partition.

---

## In-Sync Replicas

A follower is considered an **in-sync replica (ISR)** when it is sufficiently caught up with the leader according to Kafka's replication rules.

For example:

```text
Partition 0

Broker 1 → Leader       ISR
Broker 2 → Follower     ISR
Broker 3 → Follower     ISR
```

If a follower becomes unavailable or falls sufficiently behind, it can be removed from the ISR.

```text
Broker 1 → Leader       ISR
Broker 2 → Follower     ISR
Broker 3 → Unavailable
```

The ISR is important because Kafka uses it when determining which replicas can participate in durability guarantees and leader election.

Producer acknowledgement behavior involving the ISR is covered in [Kafka Producer](02-kafka-producer.md).

---

## Replication Factor vs Partitions

Partitions and replicas solve different problems.

Consider:

```text
partitions = 3
replication.factor = 3
```

There are three logical partition logs:

```text
Partition 0
Partition 1
Partition 2
```

Each partition also has three copies.

A possible distribution is:

```text
                Broker 1     Broker 2     Broker 3

Partition 0     Leader       Follower     Follower
Partition 1     Follower     Leader       Follower
Partition 2     Follower     Follower     Leader
```

The cluster therefore contains nine partition replicas:

```text
3 partitions × 3 replicas = 9 partition replicas
```

but there are still only **three logical partitions**.

---

## Producers and Consumers

Kafka applications generally interact with the cluster as either producers or consumers.

A **producer** publishes records:

```text
Application
    |
    v
Producer
    |
    v
Kafka Topic
```

A **consumer** reads records:

```text
Kafka Topic
    |
    v
Consumer
    |
    v
Application
```

Producers and consumers are decoupled.

A producer does not need to know which consumer will process an event, and a consumer does not need to communicate directly with the producer.

This is one of the foundations of event-driven architecture.

Producer behavior is covered in [Kafka Producer](02-kafka-producer.md).

---

## Kafka CLI

Kafka provides command-line tools that can be used to interact directly with a cluster.

The CLI is useful for:

- Creating and inspecting topics
- Viewing topic configuration
- Publishing test records
- Consuming records
- Inspecting cluster behavior

Although production applications normally interact with Kafka through client libraries, the CLI is particularly useful during development, debugging, and operational investigation.

---

## Common Misconceptions

### Replicas are additional partitions

They are not.

A replica is a copy of an existing partition.

```text
3 partitions × replication factor 3
```

still means three logical partitions, not nine.

### Replication creates additional offsets

It does not.

Every replica maintains a copy of the same partition log and therefore the same offsets.

### Kafka guarantees ordering across a topic

Kafka guarantees ordering within a partition, not across multiple partitions.

If events must maintain relative ordering, they need to be routed appropriately, commonly by using the same message key.

### A message key is the same as an offset

They serve completely different purposes.

A key can influence partition selection and group related records.

An offset identifies a record's position within a partition.

---

## Interview Corner

### What is a Kafka broker?

A broker is a Kafka server that stores partition data and handles requests from producers and consumers.

Multiple brokers form a Kafka cluster.

### What is the difference between a topic and a partition?

A topic is a logical stream of records.

A partition is an ordered log that stores a portion of the records belonging to that topic.

A topic can contain multiple partitions.

### What is an offset?

An offset is the position of a record within a partition.

Offsets are unique within a partition, not across an entire topic.

### Why does Kafka use partitions?

Partitions allow Kafka to distribute data and processing, enabling scalability and parallelism.

They also define the scope of Kafka's ordering guarantee.

### What is the difference between a partition and a replica?

A partition is a logical ordered log.

A replica is a copy of that partition stored on another broker for fault tolerance.

### What is the difference between a leader and a follower?

The leader handles reads and writes for the partition, while followers replicate the leader's data.

If the leader becomes unavailable, an eligible replica can be elected as the new leader.

### Why are message keys important?

Message keys can be used to consistently route related records to the same partition.

This is particularly useful when ordering must be preserved for events belonging to the same entity.

---

## Rules of Thumb

- Think of a topic as a logical stream and a partition as an ordered log.
- Remember that offsets belong to partitions.
- Use partitions to scale storage and processing.
- Use replication for fault tolerance, not parallelism.
- Use message keys when related events need partition-level ordering.
- Do not assume ordering across different partitions.
- Keep the distinction between partitions, replicas, and brokers clear.