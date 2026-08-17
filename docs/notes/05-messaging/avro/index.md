# Avro

This module explores Apache Avro in the context of Kafka-based backend systems.

Kafka stores records as bytes. It does not know whether those bytes represent JSON, Avro, Protobuf, a Java object, or something else.

That means producers and consumers need a shared contract for the structure of the messages they exchange.

Avro provides a schema-based way to define that contract and evolve it over time.

## Topics

- [Avro Fundamentals](01-avro-fundamentals.md)
- [Schema Registry](02-schema-registry.md)
- [Avro with Java and Spring Kafka](03-avro-with-java.md)

## Learning Objectives

By the end of this module, you should be able to:

- Understand why Avro is commonly used with Kafka.
- Explain the difference between JSON messages and Avro binary encoding.
- Read and write basic Avro schemas.
- Understand required fields, nullable fields, defaults, enums, arrays, and maps.
- Explain writer schema, reader schema, and schema resolution.
- Understand backward, forward, and full compatibility.
- Explain why Kafka applications commonly use a Schema Registry.
- Configure Java and Spring Kafka applications to produce and consume Avro events.
- Discuss Avro trade-offs confidently in backend interviews.

## Practical Focus

The examples in this module focus on event-driven backend systems.

The goal is not only to learn Avro syntax, but to understand how Avro affects service contracts, deployments, compatibility, and long-term event design.
