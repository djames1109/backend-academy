# 05 - Messaging and Event-Driven Systems

This module explores messaging and event-driven systems commonly used in modern backend applications, with an initial focus on Apache Kafka.

Rather than treating messaging as simply sending data from one application to another, the topics explore how messages are published, stored, replicated, consumed, and processed reliably in distributed systems.

## Topics

### Apache Kafka

- [Kafka Fundamentals](01-kafka-fundamentals.md)
- [Kafka Producer](02-kafka-producer.md)
- [Kafka Consumer](03-kafka-consumer.md)]
- [ ] Consumer Groups and Partition Assignment
- [ ] Offset Management
- [ ] Delivery Semantics
- [ ] Kafka Transactions
- [ ] Error Handling and Dead Letter Topics

## Learning Objectives

By the end of this module, you should be able to:

- Understand why messaging and event-driven architectures are used in backend systems.
- Explain the core concepts behind Apache Kafka, including brokers, topics, partitions, offsets, and replication.
- Understand how Kafka producers publish records reliably.
- Understand how consumers process records and manage offsets.
- Explain how Kafka handles failures, retries, ordering, and duplicate delivery.
- Understand the trade-offs between durability, availability, latency, and throughput.
- Apply Kafka concepts when designing event-driven backend applications.
- Discuss Kafka architecture and messaging concepts confidently in technical interviews.

## Practical Project

The module includes a Spring Boot Kafka sandbox used to experiment with the concepts covered throughout the module.

The project will progressively demonstrate concepts such as:

- Publishing events with Spring Kafka
- Producer acknowledgements and retries
- Producer idempotence
- Message keys and partitioning
- Consumer groups
- Offset management
- Error handling and retries
- Reliable event processing