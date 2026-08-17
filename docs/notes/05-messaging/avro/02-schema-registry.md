# Schema Registry

## Introduction

Avro binary data requires a schema to be interpreted correctly.

In a simple file-based system, the writer schema can be stored with the data.

Kafka is different.

Kafka records are small messages moving through topics, and storing the full schema in every record would waste space.

Schema Registry solves this problem by storing schemas outside Kafka records and giving each registered schema an identifier.

The Kafka record can then carry a small schema identifier instead of the full schema.

---

## Why Kafka Alone Is Not Enough

Kafka stores bytes.

It does not validate whether those bytes match an Avro schema.

```text
Producer
   |
   v
Kafka record bytes
   |
   v
Kafka topic
```

Kafka can store the record, but it does not know:

- Which Avro schema was used
- Whether the schema is compatible with previous versions
- Whether consumers can read the record
- Whether the event contract was broken

Schema Registry adds a contract layer around Kafka messages.

```text
Producer
   |
   +--> Schema Registry
   |
   v
Kafka topic
```

---

## Schema IDs

When a schema is registered, Schema Registry assigns it an ID.

Conceptually:

```text
Schema Registry

ID 12 -> OrderCreated v1
ID 18 -> OrderCreated v2
ID 24 -> PaymentCreated v1
```

The producer uses this ID when serializing the record.

The consumer reads the ID from the record and asks Schema Registry for the writer schema.

---

## What Goes Into Kafka

With Confluent's Avro serializer, the Kafka record value commonly contains:

```text
magic byte
schema id
avro payload
```

Conceptually:

```text
Kafka record value

+------------+-------------+----------------+
| magic byte | schema id   | Avro bytes     |
+------------+-------------+----------------+
```

The full schema is not stored inside every Kafka record.

The schema ID points to the schema stored in Schema Registry.

This keeps Kafka messages compact while still allowing consumers to find the correct writer schema.

---

## Producer Flow

When a producer publishes an Avro event:

```text
Application object
      |
      v
Avro serializer
      |
      v
Check/register schema
      |
      v
Get schema ID
      |
      v
Write schema ID + Avro bytes
      |
      v
Kafka topic
```

If the schema is new, the serializer may register it, depending on configuration.

If compatibility rules are enabled, Schema Registry can reject incompatible schemas before they are used for new messages.

---

## Consumer Flow

When a consumer reads an Avro event:

```text
Kafka topic
      |
      v
Read schema ID from record
      |
      v
Fetch writer schema from Schema Registry
      |
      v
Deserialize Avro bytes
      |
      v
Application object
```

The consumer does not need the full schema inside the Kafka record.

It needs access to Schema Registry, or a local cache of schemas already fetched from it.

---

## Subjects

Schema Registry organizes schemas under subjects.

A subject is a name under which schema versions are registered.

A common convention is:

```text
<topic>-value
<topic>-key
```

For example:

```text
order-created-value
order-created-key
```

The value schema for records in `order-created` may be registered under `order-created-value`.

Subject naming matters because compatibility is checked within a subject.

If unrelated event types share the same subject, their schemas may be compared against each other incorrectly.

---

## Compatibility Modes

Schema Registry can check whether a new schema is compatible with previous schema versions.

Common compatibility modes include:

```text
BACKWARD
FORWARD
FULL
NONE
```

There are also transitive variants that check against more than the latest schema version.

---

## Backward Compatibility

Backward compatibility means a new reader can read old data.

```text
Old producer -> old schema
New consumer -> new schema
```

This is useful when consumers are upgraded before all old data has disappeared.

Adding a field with a default is commonly backward compatible.

```json
{
  "name": "currency",
  "type": "string",
  "default": "USD"
}
```

---

## Forward Compatibility

Forward compatibility means an old reader can read new data.

```text
New producer -> new schema
Old consumer -> old schema
```

This is useful when producers may be upgraded before all consumers are upgraded.

Forward compatibility matters in Kafka because multiple consumers may read the same topic.

Some consumers may be updated later than others.

---

## Full Compatibility

Full compatibility means both backward and forward compatibility.

```text
New readers can read old data.
Old readers can read new data.
```

This is often a good default goal for shared business events.

It forces schema changes to be safer across independent deployments.

---

## Transitive Compatibility

Non-transitive compatibility commonly checks a new schema against the latest previous version.

Transitive compatibility checks against all previous versions required by the mode.

Conceptually:

```text
Schema v1
Schema v2
Schema v3
Schema v4
```

With a transitive mode, `v4` may need to be compatible with `v1`, `v2`, and `v3`, not only `v3`.

This is useful when very old data may still exist in Kafka or in replayable storage.

---

## Auto Registration

Many serializers can automatically register a schema when a producer sends a record.

This is convenient during development.

In production, teams often prefer more control.

For example:

- Register schemas during CI/CD.
- Reject incompatible schemas before application deployment.
- Disable automatic registration in producer applications.
- Require schema review for shared topics.

The right choice depends on team maturity and deployment process.

The important point is that schema changes should be deliberate.

---

## Schema Registry Is Not a Database Constraint

Schema Registry checks schema compatibility.

It does not understand all business rules.

For example, a schema can say:

```json
{
  "name": "amount",
  "type": "double"
}
```

But it does not know whether `amount` must be greater than zero.

That kind of rule still belongs in application validation.

Schema compatibility and business validation solve different problems.

---

## Common Mistakes

### Thinking Kafka validates Avro schemas

Kafka stores bytes.

Schema validation and compatibility checks are handled by serializers, deserializers, and Schema Registry.

### Assuming the full schema is inside every Kafka record

With the common Schema Registry wire format, the record carries a schema ID and Avro payload, not the full schema.

### Using the wrong subject strategy

Subject naming affects which schemas are compared for compatibility.

Use a strategy that matches how the topic is intended to evolve.

### Leaving auto registration uncontrolled

Automatic schema registration is convenient, but it can allow accidental schema changes to reach shared environments.

### Treating compatibility as business validation

Compatibility checks whether schemas can read each other's data.

They do not enforce every domain rule.

---

## Interview Corner

### Why is Schema Registry used with Avro and Kafka?

Avro binary data needs a schema to be read correctly.

Schema Registry stores schemas centrally and allows Kafka messages to carry a compact schema ID instead of the full schema.

### What is a schema ID?

A schema ID identifies a registered schema.

Consumers use it to look up the writer schema needed to deserialize a record.

### Does Kafka enforce schema compatibility?

No.

Kafka stores bytes.

Schema compatibility is enforced by the schema tooling around Kafka, commonly Schema Registry and serializers.

### What is a subject?

A subject is the name under which schema versions are registered and checked for compatibility.

For Kafka topics, common subjects are based on topic keys and values.

### What is full compatibility?

Full compatibility means new readers can read old data and old readers can read new data.

---

## Rules of Thumb

- Use Schema Registry when Avro schemas must evolve across services.
- Treat subject naming as part of event design.
- Prefer compatibility checks before schemas reach production.
- Use full compatibility for shared business events when independent deployment matters.
- Use transitive compatibility when old data may be replayed for a long time.
- Do not rely on Schema Registry for business validation.
- Remember that Kafka stores bytes; Schema Registry manages schemas.
