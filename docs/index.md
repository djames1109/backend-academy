# Backend Academy

Backend Academy is a collection of notes, examples, and deep dives covering modern Java and backend engineering.

The goal is to build a solid understanding of the technologies commonly used in modern backend systems, including Java, concurrency, JVM internals, Spring, Quarkus, AWS, databases, distributed systems, networking, and Kubernetes.

Rather than focusing only on syntax or interview preparation, each topic explores the motivation behind a feature, how it works internally, and how it is applied in real-world backend development.

Although these notes were originally created as part of my own learning journey, they are written as a practical reference for anyone interested in modern Java and backend development.

## Learning Philosophy

Every topic follows the same approach:

1. Why does this exist?
2. What problem does it solve?
3. How does it work internally?
4. When should it be used?
5. When should it not be used?
6. How is it used in real backend systems?
7. How would you explain it in an interview?

## Roadmap

### [01 - Modern Java](notes/01-modern-java/index.md)

- [Records](notes/01-modern-java/01-records.md)
- [Sealed Classes](notes/01-modern-java/02-sealed-classes.md)
- [ ] Pattern Matching for instanceof
- [ ] Switch Expressions
- [ ] Pattern Matching for switch
- [ ] Record Patterns
- [ ] Virtual Threads
- [ ] Structured Concurrency
- [ ] Scoped Values
- [ ] Sequenced Collections

### 02 - Concurrency

- [ ] Threads
- [ ] ExecutorService
- [ ] CompletableFuture
- [ ] Atomic Classes
- [ ] synchronized
- [ ] Locks
- [ ] ConcurrentHashMap
- [ ] Virtual Threads in backend services

### 03 - JVM Internals

- [ ] Heap and Stack
- [ ] Garbage Collection
- [ ] G1GC
- [ ] ZGC
- [ ] JIT Compiler
- [ ] Class Loading
- [ ] Reflection
- [ ] Escape Analysis

### 04 - Spring and Quarkus Internals

- [ ] Dependency Injection
- [ ] Bean Lifecycle
- [ ] AOP and Proxies
- [ ] Transactions
- [ ] Validation
- [ ] Configuration
- [ ] Reflection vs Build-time Processing

### [05 - Messaging and Event-Driven Systems](notes/05-messaging/index.md)

- [Kafka Fundamentals](notes/05-messaging/01-kafka-fundamentals.md)
- [Kafka Producer](notes/05-messaging/02-kafka-producer.md)
- [Kafka Consumer](notes/05-messaging/03-kafka-consumer.md)
- [ ] Consumer Groups and Partition Assignment
- [ ] Offset Management
- [ ] Delivery Semantics
- [Kafka Transactions](notes/05-messaging/04-kafka-transaction.md)
- [ ] Error Handling and Dead Letter Topics

### 06 - AWS

- [ ] IAM
- [ ] Lambda
- [ ] API Gateway
- [ ] SQS
- [ ] SNS
- [ ] EventBridge
- [ ] Step Functions
- [ ] DynamoDB
- [ ] RDS
- [ ] CloudWatch

### 07 - System Design

- [ ] Scalability
- [ ] Availability
- [ ] Load Balancing
- [ ] Caching
- [ ] Idempotency
- [ ] Retry Patterns
- [ ] Saga Pattern
- [ ] Outbox Pattern
- [ ] CQRS
- [ ] Event Sourcing

### 08 - Databases

- [ ] Indexes
- [ ] B-Trees
- [ ] Query Plans
- [ ] Transactions
- [ ] Isolation Levels
- [ ] Locks
- [ ] Deadlocks
- [ ] Partitioning
- [ ] Replication

### 09 - Networking

- [ ] HTTP
- [ ] DNS
- [ ] TCP
- [ ] TLS
- [ ] REST
- [ ] gRPC
- [ ] WebSockets
- [ ] Server-Sent Events

### 10 - Kubernetes

- [ ] Pods
- [ ] Deployments
- [ ] Services
- [ ] Ingress
- [ ] ConfigMaps
- [ ] Secrets
- [ ] HPA
- [ ] Rolling Updates
