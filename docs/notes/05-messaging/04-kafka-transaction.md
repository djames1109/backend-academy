# Kafka Transactions

## Introduction

Kafka transactions allow multiple Kafka operations to be treated as a single atomic unit.

Consider a service that consumes a money transfer event and publishes two new events:

```text
transfer-topic
      |
      v
Transfer Service
      |
      ├── withdraw-topic
      |
      └── deposit-topic
```

Publishing only one of these events could leave the system in an inconsistent state.

The desired behavior is:

```text
withdraw published ✅
deposit published  ✅
```

or:

```text
withdraw published ❌
deposit published  ❌
```

but never:

```text
withdraw published ✅
deposit published  ❌
```

Kafka transactions provide atomicity across Kafka operations performed within the same transaction.

Transactions become more complicated when an operation also modifies another transactional resource such as a relational database.

Understanding that distinction is essential when designing transactional Kafka applications.

---

## Kafka-Only Transactions

Consider a method that publishes two records:

```java
@Transactional("kafkaTransactionManager")
public void processTransfer(TransferEvent event) {

    kafkaTemplate.send(
        "withdraw-topic",
        createWithdrawal(event)
    );

    kafkaTemplate.send(
        "deposit-topic",
        createDeposit(event)
    );
}
```

Both sends participate in the same Kafka transaction.

Conceptually:

```text
BEGIN KAFKA TRANSACTION

    publish withdrawal

    publish deposit

COMMIT KAFKA TRANSACTION
```

If processing succeeds:

```text
withdraw-topic    ✅
deposit-topic     ✅
```

If an exception causes the transaction to abort:

```text
withdraw-topic    ❌
deposit-topic     ❌
```

The two calls to `send()` are not independently committed Kafka transactions.

They are writes participating in one Kafka transaction.

---

## Transactional Producers

Normal Kafka producers can publish records without participating in transactions.

To use Kafka transactions, the producer must be configured as a transactional producer.

Spring Kafka manages transactional producers through `DefaultKafkaProducerFactory`.

In Spring Boot, this can be enabled by configuring a transaction ID prefix.

For example:

```yaml
spring:
  kafka:
    producer:
      transaction-id-prefix: transfer-service-
```

This allows Spring Kafka to create transactional producers.

Conceptually:

```text
transaction-id-prefix
        |
        v
DefaultKafkaProducerFactory
        |
        v
Transactional Kafka Producers
```

---

## Transactional ID

A transactional Kafka producer is configured with a `transactional.id`.

It is important not to confuse this with a business transaction identifier.

For example:

```text
transferId = "TRANSFER-123"
```

might identify a business operation.

A Kafka transactional ID instead identifies a **transactional producer**.

Conceptually:

```text
Transactional Producer

transactional.id =
transfer-service-0
```

That producer can execute many Kafka transactions during its lifetime:

```text
Producer: transfer-service-0

Transaction A
    BEGIN
    send
    send
    COMMIT

Transaction B
    BEGIN
    send
    send
    COMMIT

Transaction C
    BEGIN
    send
    ABORT
```

The `transactional.id` identifies the producer across those transactions.

It does not identify Transaction A, B, or C individually.

---

## transaction-id-prefix

Spring manages multiple transactional producer instances internally.

Rather than manually assigning every producer a transactional ID, an application can provide a prefix:

```yaml
spring:
  kafka:
    producer:
      transaction-id-prefix: transfer-service-
```

Spring can use this prefix when creating transactional producer identities.

Conceptually:

```text
transfer-service-0
transfer-service-1
transfer-service-2
...
```

When multiple application instances are running simultaneously, their transactional producer identities must remain unique.

For example:

```text
Instance A

transfer-service-instance-a-0


Instance B

transfer-service-instance-b-0
```

Reusing the same transactional producer identity across active instances can result in Kafka fencing an older producer.

---

## Producer Fencing

Kafka uses transactional producer identities to protect against stale producer instances.

Imagine an application instance becomes temporarily disconnected:

```text
Producer A
transactional.id = transfer-service-1
```

Another instance starts using the same transactional identity.

Kafka must prevent the old producer from later reconnecting and continuing to write as if it were still the valid producer.

The newer producer can therefore fence the older producer.

Conceptually:

```text
Producer A
transactional.id = transfer-service-1
        |
        X fenced


Producer B
transactional.id = transfer-service-1
        |
        v
current producer
```

This prevents multiple producer instances from independently operating under the same transactional identity.

---

## Consuming Transactional Records

Kafka stores records involved in transactions in its log before the transaction necessarily commits.

This creates an important consumer-side question:

> Should consumers be allowed to see records from transactions that eventually abort?

Kafka provides the consumer property:

```properties
isolation.level
```

Two important values are:

```text
read_uncommitted
read_committed
```

---

### read_uncommitted

With:

```properties
isolation.level=read_uncommitted
```

a consumer may read transactional records even if the transaction later aborts.

Consider:

```text
BEGIN TRANSACTION

publish withdrawal   ✅ written

publish deposit      ❌ failure

ABORT TRANSACTION
```

The withdrawal record physically reached the Kafka log.

A `read_uncommitted` consumer may therefore observe it even though the overall transaction was aborted.

---

### read_committed

Transactional consumers commonly use:

```properties
isolation.level=read_committed
```

This means:

> Only expose records from successfully committed transactions.

Using the previous example:

```text
BEGIN

withdrawal written
deposit fails

ABORT
```

a `read_committed` consumer will not return the withdrawal record as part of normal consumption.

Conceptually:

```text
Kafka Log

Record A ── committed transaction ──> visible

Record B ── aborted transaction ────> hidden
```

This completes the atomicity story from the consumer's perspective.

---

## Spring Transaction Management

`@Transactional` is not specific to databases.

It is part of Spring's transaction abstraction.

For example:

```java
@Transactional
public void process() {
    ...
}
```

conceptually means:

> Execute this method within a transaction managed by a Spring transaction manager.

Spring defines a transaction-management abstraction implemented by different transaction managers.

Examples include:

```text
JpaTransactionManager
KafkaTransactionManager
DataSourceTransactionManager
JmsTransactionManager
JtaTransactionManager
```

Each transaction manager understands how to manage transactions for a particular type of resource.

---

## Transaction Managers

A useful mental model is:

```text
                  Spring Transaction Infrastructure
                             |
                             v
                  PlatformTransactionManager
                             |
          ┌──────────────────┼──────────────────┐
          |                  |                  |
          v                  v                  v
JpaTransactionManager  KafkaTransactionManager  ...
          |                  |
          v                  v
      Database             Kafka
```

Spring provides the transaction abstraction and lifecycle infrastructure.

The transaction manager performs the actual resource-specific work.

For example:

```text
Spring
   |
   | begin()
   v
JpaTransactionManager
   |
   v
BEGIN DATABASE TRANSACTION
```

or:

```text
Spring
   |
   | begin()
   v
KafkaTransactionManager
   |
   v
BEGIN KAFKA TRANSACTION
```

---

## Multiple Transaction Managers

An application may contain more than one transaction manager.

For example:

```java
@Bean("transactionManager")
JpaTransactionManager transactionManager(...) {
    ...
}

@Bean("kafkaTransactionManager")
KafkaTransactionManager<?, ?> kafkaTransactionManager(...) {
    ...
}
```

Now the application has:

```text
transactionManager
        |
        v
JpaTransactionManager


kafkaTransactionManager
        |
        v
KafkaTransactionManager
```

When multiple transaction managers exist, the application may explicitly select one:

```java
@Transactional("transactionManager")
```

or:

```java
@Transactional("kafkaTransactionManager")
```

The selected transaction manager defines the primary transaction boundary for that method.

---

## JpaTransactionManager

`JpaTransactionManager` manages database transactions through JPA.

For example:

```java
@Transactional("transactionManager")
public void createProduct() {

    repository.save(...);

    repository.update(...);
}
```

Conceptually:

```text
@Transactional
       |
       v
JpaTransactionManager
       |
       v
BEGIN DB TRANSACTION
       |
       ├── INSERT
       └── UPDATE
       |
       v
COMMIT
```

If the method fails before completion:

```text
ROLLBACK
```

---

## KafkaTransactionManager

`KafkaTransactionManager` manages Kafka producer transactions.

For example:

```java
@Transactional("kafkaTransactionManager")
public void publishTransfer() {

    kafkaTemplate.send("withdraw-topic", ...);

    kafkaTemplate.send("deposit-topic", ...);
}
```

Conceptually:

```text
@Transactional
       |
       v
KafkaTransactionManager
       |
       v
BEGIN KAFKA TRANSACTION
       |
       ├── send withdrawal
       └── send deposit
       |
       v
COMMIT
```

If processing fails:

```text
ABORT KAFKA TRANSACTION
```

---

## Transaction Managers Manage Their Own Resources

One of the most important concepts is:

> A transaction manager understands and manages its own transactional resource.

For example:

```text
JpaTransactionManager
        |
        └── Database


KafkaTransactionManager
        |
        └── Kafka
```

`JpaTransactionManager` does not directly implement Kafka transactions.

Likewise:

```text
KafkaTransactionManager
```

does not directly manage JPA database transactions.

This distinction becomes important when a method uses both resources.

---

## Database and Kafka in the Same Operation

Consider:

```java
@Transactional("transactionManager")
public void processTransfer() {

    repository.save(...);

    kafkaTemplate.send(
        "withdraw-topic",
        ...
    );

    kafkaTemplate.send(
        "deposit-topic",
        ...
    );
}
```

The method interacts with:

```text
Database
+
Kafka
```

If `transactionManager` refers to `JpaTransactionManager`, then the database transaction is the primary Spring transaction.

Conceptually:

```text
@Transactional("transactionManager")
               |
               v
       JpaTransactionManager
               |
               v
       BEGIN DB TRANSACTION
```

However, a transaction-capable `KafkaTemplate` can synchronize Kafka transactional work with the existing Spring transaction.

---

## Kafka Transaction Synchronization

Spring Kafka provides integration that allows a transactional `KafkaTemplate` to synchronize a Kafka transaction with another active Spring transaction.

This is an important implementation detail.

The flow can look like:

```text
@Transactional("transactionManager")
               |
               v
       JpaTransactionManager
               |
               v
       BEGIN DB TRANSACTION
               |
               v
       repository.save()
               |
               v
       kafkaTemplate.send()
               |
               v
Spring Kafka detects the active transaction
               |
               v
Kafka transactional work is synchronized
               |
               v
        method completes
```

There are still two transactional resources:

```text
Database transaction
+
Kafka transaction
```

They are coordinated by Spring's transaction infrastructure and Spring Kafka integration.

---

## Why Kafka Can Synchronize with JPA

This behavior should not be interpreted as:

> Any Spring transaction manager automatically joins any other transaction manager.

That is not generally true.

Spring Kafka explicitly provides integration for transactional Kafka operations to synchronize with an existing Spring-managed transaction.

For example:

```text
Existing JPA transaction
        |
        v
Transactional KafkaTemplate
        |
        v
Kafka transaction synchronized
```

This is behavior provided by the Spring Kafka integration.

It is not a universal capability of `PlatformTransactionManager`.

---

## Different Transaction Managers Do Not Automatically Join

Consider an application containing:

```text
JpaTransactionManager
JmsTransactionManager
```

If there is no explicit integration coordinating those resources:

```java
@Transactional("transactionManager")
public void process() {

    repository.save(...);

    sendJmsMessage(...);
}
```

selecting the JPA transaction manager does not automatically cause a JMS transaction manager to join that database transaction.

Likewise, simply moving the JMS call into another method:

```java
@Transactional("jmsTransactionManager")
public void sendMessage() {
    ...
}
```

does not make it part of the same database transaction.

Instead, there are two transaction boundaries managed by different transaction managers.

The general rule is:

> Using `@Transactional` does not automatically combine different transaction managers into one transaction.

Cross-resource coordination must be explicitly supported or deliberately designed.

---

## Same Transaction Manager vs Different Transaction Managers

When nested transactional methods use the same transaction manager and compatible propagation settings, the inner method can participate in the existing transaction.

Conceptually:

```text
@Transactional(JPA)
        |
        v
Outer Method
        |
        v
@Transactional(JPA)
        |
        v
same JPA transaction
```

But:

```text
@Transactional(JPA)
        |
        v
Outer Method
        |
        v
@Transactional(Kafka)
```

does not mean:

```text
one transaction
```

They are different transactional resources.

This distinction is fundamental when working with multiple transaction managers.

---

## Database + Kafka Commit Order

Synchronizing Kafka with a database transaction provides useful coordination, but the two resources still have separate commits.

In the common database-first arrangement:

```text
Application method succeeds
        |
        v
COMMIT DATABASE
        |
        v
COMMIT KAFKA
```

This works well when both commits succeed.

However, it introduces an important failure scenario.

---

## The Second-Commit Problem

Consider:

```text
COMMIT DATABASE   ✅

COMMIT KAFKA      ❌
```

The database transaction has already been committed.

Kafka then fails during its commit.

At this point, the application cannot simply roll back the already committed database transaction.

In current Spring Kafka versions, this commit failure is propagated to the caller so the application can take compensating or remedial action.

The result may temporarily be:

```text
Database
    |
    └── change exists ✅


Kafka
    |
    └── event not successfully committed ❌
```

This demonstrates an important limitation:

> Synchronized database and Kafka transactions are not the same as one distributed ACID transaction.

---

## Why It Is Not One Global Transaction

From application code, this may look like one operation:

```java
@Transactional
public void process() {

    repository.save(...);

    kafkaTemplate.send(...);
}
```

During normal execution, failures may also appear to behave atomically:

```text
method throws
     |
     ├── DB rollback
     └── Kafka abort
```

However, there are actually separate resource transactions:

```text
Database Transaction
        |
        └── JpaTransactionManager


Kafka Transaction
        |
        └── Kafka transactional infrastructure
```

There is no single global commit operation that guarantees:

```text
COMMIT DB + KAFKA
```

as one indivisible operation.

Therefore, failures during the commit phase must still be considered.

---

## Idempotency and Commit Failures

Suppose:

```text
First attempt

Database commit   ✅
Kafka commit      ❌
```

The original Kafka record may later be delivered again.

The second attempt could then execute:

```text
Database operation
Kafka publishing
```

again.

Without idempotency, the already committed database operation could be duplicated.

An idempotent implementation can instead detect the previous processing:

```text
Second attempt

Check idempotency key
        |
        v
Already persisted
        |
        v
Skip duplicate DB side effect
        |
        v
Retry Kafka publication
```

This allows the application to recover from certain partial commit scenarios safely.

---

## Transactions Do Not Replace Idempotency

Kafka transactions provide strong guarantees, but transactional applications should still consider idempotency.

These mechanisms solve related but different problems.

```text
Kafka Transaction
        |
        v
Atomic Kafka operations


Database Transaction
        |
        v
Atomic database operations


Idempotency
        |
        v
Safe repeated processing
```

Together, they make failure recovery significantly safer.

---

## Transactions and Consumer Retries

Transactions become especially important when a Kafka listener performs multiple side effects.

For example:

```text
consume TransferEvent
        |
        v
update database
        |
        v
publish withdrawal
        |
        v
publish deposit
```

If processing fails and the Kafka record is redelivered, the application must ensure that repeating the operation does not corrupt state.

This connects several Kafka concepts:

```text
Consumer retries
        +
Transactions
        +
Idempotency
        +
DLT / recovery
```

Reliable event-driven systems usually need to consider these concepts together rather than independently.

---

## Common Mistakes

### Assuming @Transactional means database transaction

`@Transactional` belongs to Spring's transaction abstraction.

The actual resource being managed depends on the transaction manager selected for that method.

---

### Assuming KafkaTransactionManager manages JPA

It does not.

`KafkaTransactionManager` manages Kafka transactional producers.

Database transactions require an appropriate database transaction manager.

---

### Assuming JpaTransactionManager manages Kafka

`JpaTransactionManager` manages JPA/database transactions.

Transactional Kafka work can synchronize with an existing Spring transaction through Spring Kafka's integration, but this does not mean `JpaTransactionManager` itself understands Kafka.

---

### Assuming all transaction managers automatically synchronize

They do not.

Synchronization between different transactional resources requires explicit framework support or application architecture.

Spring Kafka provides specific support for synchronizing transactional Kafka operations with an existing Spring transaction.

---

### Confusing transactional.id with a business transaction ID

The Kafka `transactional.id` identifies a transactional producer.

It does not identify an individual business operation or individual Kafka transaction.

---

### Forgetting read_committed

A transactional producer alone does not determine what consumers can observe.

Consumers that should only process committed transactional records should use the appropriate isolation level.

---

### Assuming DB + Kafka is one distributed transaction

Synchronizing database and Kafka transactions does not create one globally atomic commit.

One resource may successfully commit before another resource fails.

Applications must account for this possibility.

---

### Assuming transactions eliminate duplicate processing

Transactions improve atomicity.

They do not remove every scenario in which an operation may be retried or redelivered.

Idempotency remains an important design principle.

---

## Interview Corner

### What problem do Kafka transactions solve?

Kafka transactions allow multiple Kafka operations to be committed or aborted atomically.

This prevents consumers using `read_committed` from observing only part of a logical set of Kafka writes.

---

### What is Kafka's transactional.id?

`transactional.id` identifies a transactional Kafka producer.

Kafka uses this identity to manage transactional state and protect against stale producers.

It is not the identifier of an individual business transaction.

---

### What is transaction-id-prefix in Spring Kafka?

It provides a prefix that Spring's producer factory uses when creating transactional producer identities.

This allows Spring to manage transactional Kafka producers on behalf of the application.

---

### What does isolation.level=read_committed do?

It prevents the consumer from returning records belonging to aborted Kafka transactions.

This allows consumers to observe only successfully committed transactional writes.

---

### What is KafkaTransactionManager?

`KafkaTransactionManager` is Spring Kafka's transaction manager for Kafka producer transactions.

It allows Kafka transactions to participate in Spring's declarative transaction infrastructure.

---

### What is JpaTransactionManager?

`JpaTransactionManager` manages database transactions performed through JPA.

It controls database transaction begin, commit, and rollback operations.

---

### Does JpaTransactionManager manage Kafka transactions?

No.

Kafka transactions are managed by Kafka transactional infrastructure.

Spring Kafka can synchronize Kafka transactional work with an existing Spring transaction, but that does not make Kafka part of `JpaTransactionManager`.

---

### Does KafkaTransactionManager manage database transactions?

No.

It manages Kafka transactions.

Database work requires an appropriate database transaction manager.

---

### If two methods use different transaction managers, do they automatically share one transaction?

No.

Different transaction managers manage different transactional resources.

Using `@Transactional` on both methods does not automatically create a single transaction across those resources.

---

### Are synchronized Kafka and database transactions fully atomic?

Not globally.

The resources still perform separate commits.

One resource can commit successfully before the other resource's commit fails.

Applications should account for this possibility using strategies such as idempotency or compensation.

---

## Rules of Thumb

- Use Kafka transactions when multiple Kafka operations must succeed or fail together.
- Configure transactional producer identities when using Kafka transactions.
- Treat `transactional.id` as a producer identity, not a business transaction identifier.
- Use `read_committed` when consumers should only observe successfully committed Kafka transactions.
- Remember that `@Transactional` is a Spring transaction abstraction, not a database-specific annotation.
- Know which transaction manager is selected for every transactional boundary.
- Treat `JpaTransactionManager` and `KafkaTransactionManager` as managers of different resources.
- Do not assume different transaction managers automatically participate in one transaction.
- Understand that Spring Kafka provides explicit synchronization support with existing Spring transactions.
- Do not confuse synchronized local transactions with a globally atomic distributed transaction.
- Design for the possibility that one resource commits before another commit fails.
- Combine transaction handling with idempotency and appropriate retry/recovery strategies.
