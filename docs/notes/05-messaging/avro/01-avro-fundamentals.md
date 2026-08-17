# Avro Fundamentals

## Introduction

Apache Avro is a schema-based serialization system commonly used with Kafka.

Kafka stores records as bytes.

It does not know whether those bytes represent JSON, Avro, Protobuf, a Java object, or something else.

This means the producer and consumer need a shared understanding of the record structure.

With plain JSON, that structure is often implicit.

```json
{
  "orderId": "ORD-123",
  "customerId": "CUS-456",
  "amount": 100.50
}
```

JSON is readable, but JSON alone does not enforce:

- Which fields exist
- Which fields are required
- What types those fields have
- What default values should be used
- Which schema changes are compatible
- Whether old consumers can read new events
- Whether new consumers can read old events

Avro makes the event structure explicit through a schema.

```json
{
  "type": "record",
  "name": "OrderCreated",
  "namespace": "com.example.orders",
  "fields": [
    { "name": "orderId", "type": "string" },
    { "name": "customerId", "type": "string" },
    { "name": "amount", "type": "double" }
  ]
}
```

The schema becomes the contract between producers and consumers.

Avro is especially useful with Kafka because producers and consumers often evolve independently.

A producer may deploy a new event version before every consumer has been updated.

Avro helps teams evolve event contracts deliberately instead of relying only on informal JSON conventions.

---

## Why Avro Exists

In an event-driven system, many services may depend on the same event.

```text
Order Service
     |
     v
order-created topic
     |
     +--> Inventory Service
     +--> Notification Service
     +--> Analytics Service
```

If `Order Service` changes the event structure, all consumers are affected.

With informal JSON contracts, the change may only be discovered when a consumer fails at runtime.

For example, suppose the producer changes:

```json
{
  "amount": 100.50
}
```

to:

```json
{
  "amount": "100.50"
}
```

The payload still looks like valid JSON.

But the event contract has changed from a number to a string.

Consumers expecting a number may fail.

Avro addresses this by making the schema explicit.

The schema says what the event is supposed to contain.

Compatibility tools can then check whether a schema change is safe before producers start publishing with it.

---

## JSON vs Avro

JSON messages include field names in every payload.

```json
{
  "orderId": "ORD-123",
  "customerId": "CUS-456",
  "amount": 100.50
}
```

This is easy to inspect manually.

However, the field names and structure are repeated in every message.

Avro schemas are written as JSON documents, but Avro records are commonly serialized as compact binary data.

The schema defines the fields:

```json
{
  "type": "record",
  "name": "OrderCreated",
  "fields": [
    { "name": "orderId", "type": "string" },
    { "name": "customerId", "type": "string" },
    { "name": "amount", "type": "double" }
  ]
}
```

The Avro binary payload can then encode the values according to the schema.

```text
ORD-123
CUS-456
100.50
```

The binary payload is compact because it does not need to repeat the field names in the same way JSON does.

The trade-off is that Avro binary data requires the schema to be interpreted correctly.

```text
Avro bytes
    +
Writer schema
    |
    v
Readable data
```

---

## Schema Basics

An Avro schema describes the structure of the data.

For Kafka events, the most common schema type is a `record`.

A record is similar to a Java DTO or a JSON object with declared fields.

```json
{
  "type": "record",
  "name": "OrderCreated",
  "namespace": "com.example.orders",
  "fields": [
    {
      "name": "orderId",
      "type": "string"
    },
    {
      "name": "customerId",
      "type": "string"
    },
    {
      "name": "amount",
      "type": "double"
    }
  ]
}
```

Important record properties include:

- `type`: The kind of schema. For event objects, this is usually `record`.
- `name`: The record name.
- `namespace`: A namespace used to avoid name collisions.
- `fields`: The list of fields in the record.

Each field has a `name` and a `type`.

Avro types can be grouped into two broad categories:

```text
Primitive types
Complex types
```

Primitive types represent single simple values.

Complex types describe structured values such as records, arrays, maps, enums, unions, and fixed-size binary values.

---

## Primitive Types

Primitive types are the simplest Avro types.

Common primitive types include:

```text
string
int
long
float
double
boolean
bytes
null
```

Examples:

```json
{
  "name": "orderId",
  "type": "string"
}
```

```json
{
  "name": "amount",
  "type": "double"
}
```

```json
{
  "name": "createdAtEpochMillis",
  "type": "long"
}
```

Primitive types are useful for simple values, but real event contracts often need more structure.

That is where complex types are used.

---

## Required and Nullable Fields

By default, fields are required.

```json
{
  "name": "orderId",
  "type": "string"
}
```

This field must contain a string value.

Avro does not allow `null` unless `null` is explicitly part of the field type.

Nullable fields are commonly modeled with a union.

```json
{
  "name": "couponCode",
  "type": ["null", "string"],
  "default": null
}
```

This means the field may be either `null` or a `string`.

Avro does not have an `optional` keyword.

Optionality is modeled through a union that includes `null`.

---

## Defaults

Defaults are important in Avro because they are compatibility tools.

```json
{
  "name": "currency",
  "type": "string",
  "default": "USD"
}
```

If an older record does not contain `currency`, a newer reader can use `"USD"`.

When using `default: null`, put `null` first in the union.

```json
{
  "name": "couponCode",
  "type": ["null", "string"],
  "default": null
}
```

The default value should match the first branch of the union.

This rule becomes especially important during schema evolution.

---

## Complex Types

Complex types describe structured data.

Common Avro complex types include:

```text
record
enum
array
map
union
fixed
```

Most Kafka event schemas are built from a `record` at the top level, with other complex types used inside the record fields.

---

## Records

A `record` groups named fields together.

This is the most common top-level Avro type for Kafka events.

```json
{
  "type": "record",
  "name": "OrderCreated",
  "namespace": "com.example.orders",
  "fields": [
    { "name": "orderId", "type": "string" },
    { "name": "customerId", "type": "string" }
  ]
}
```

Records can also be nested inside other records.

```json
{
  "type": "record",
  "name": "OrderCreated",
  "namespace": "com.example.orders",
  "fields": [
    {
      "name": "orderId",
      "type": "string"
    },
    {
      "name": "shippingAddress",
      "type": {
        "type": "record",
        "name": "Address",
        "fields": [
          { "name": "street", "type": "string" },
          { "name": "city", "type": "string" },
          { "name": "country", "type": "string" }
        ]
      }
    }
  ]
}
```

Nested records are useful when a group of fields clearly belongs together.

However, deeply nested event schemas can become harder to evolve and consume.

Use nesting when it improves the event contract, not just to mirror internal domain objects.

---

## Enums

Avro supports enums for fields with a controlled set of values.

```json
{
  "name": "status",
  "type": {
    "type": "enum",
    "name": "OrderStatus",
    "symbols": ["CREATED", "PAID", "CANCELLED"]
  }
}
```

Enums should be used carefully.

Adding a new enum symbol can break older consumers that do not recognize the new value.

For example:

```json
{
  "type": "enum",
  "name": "OrderStatus",
  "symbols": ["CREATED", "PAID", "CANCELLED", "REFUNDED"]
}
```

An older consumer that only knows `CREATED`, `PAID`, and `CANCELLED` may not know how to handle `REFUNDED`.

If an enum may evolve, consider including an `UNKNOWN` symbol and a default.

```json
{
  "type": "enum",
  "name": "OrderStatus",
  "symbols": ["UNKNOWN", "CREATED", "PAID", "CANCELLED"],
  "default": "UNKNOWN"
}
```

If values change frequently or come from an external system, a `string` may be safer than an Avro enum.

---

## Arrays

Avro supports arrays.

```json
{
  "name": "items",
  "type": {
    "type": "array",
    "items": "string"
  }
}
```

The `items` property defines the type of each element.

Arrays can contain primitive values:

```json
{
  "name": "tags",
  "type": {
    "type": "array",
    "items": "string"
  },
  "default": []
}
```

Arrays can also contain records:

```json
{
  "name": "items",
  "type": {
    "type": "array",
    "items": {
      "type": "record",
      "name": "OrderItem",
      "fields": [
        { "name": "productId", "type": "string" },
        { "name": "quantity", "type": "int" }
      ]
    }
  },
  "default": []
}
```

When adding an array field, a default empty array is often useful for compatibility.

---

## Maps

Avro supports maps.

```json
{
  "name": "metadata",
  "type": {
    "type": "map",
    "values": "string"
  }
}
```

Avro map keys are strings.

The `values` property defines the type of the map values.

Maps are useful for simple metadata.

```json
{
  "name": "metadata",
  "type": {
    "type": "map",
    "values": "string"
  },
  "default": {}
}
```

Avoid using maps as a replacement for well-defined event fields.

If a field is important to consumers, model it explicitly in the schema.

---

## Unions

A union means a value may be one of several types.

The most common union in Avro is a nullable field.

```json
{
  "name": "couponCode",
  "type": ["null", "string"],
  "default": null
}
```

This means `couponCode` may be either `null` or a `string`.

Unions can contain more than two branches, but they should be used carefully.

```json
{
  "name": "paymentReference",
  "type": ["null", "string", "long"],
  "default": null
}
```

This kind of schema can be harder for application code to handle.

For Kafka event contracts, unions are usually clearest when used for nullable fields.

---

## Fixed

The `fixed` type represents a fixed number of bytes.

```json
{
  "name": "traceId",
  "type": {
    "type": "fixed",
    "name": "TraceId",
    "size": 16
  }
}
```

This can be useful for binary identifiers, hashes, or values that must always have a specific byte length.

For normal business fields, `string`, `bytes`, or a more explicit record is often easier to work with.

---

## Logical Types

Avro logical types add meaning to primitive or complex types.

For example, a timestamp may be physically encoded as a `long`, but interpreted as milliseconds from the Unix epoch.

```json
{
  "name": "createdAt",
  "type": {
    "type": "long",
    "logicalType": "timestamp-millis"
  }
}
```

Common logical types include:

```text
date
time-millis
timestamp-millis
timestamp-micros
uuid
decimal
```

Logical types are useful because they make the intended meaning clearer than using only a primitive type.

For money, avoid using `double` when exact decimal precision matters.

Avro decimal values are commonly represented with `bytes` and a decimal logical type.

```json
{
  "name": "amount",
  "type": {
    "type": "bytes",
    "logicalType": "decimal",
    "precision": 12,
    "scale": 2
  }
}
```

This represents a decimal number with up to 12 total digits and 2 digits after the decimal point.

Use logical types when the primitive type alone does not communicate enough meaning.

---

## Binary Encoding

Avro schemas are written as JSON documents, but Avro records are commonly serialized as compact binary data.

Kafka stores records as bytes.

When a producer sends Avro data to Kafka, the serializer converts the application object into Avro bytes.

```text
Producer object
     |
     v
Avro serializer
     |
     v
Avro bytes
     |
     v
Kafka topic
```

A consumer then needs an Avro deserializer to convert those bytes back into an application object.

```text
Kafka topic
     |
     v
Avro bytes
     |
     v
Avro deserializer
     |
     v
Consumer object
```

The deserializer needs to know which schema was used to write the data.

This leads to the distinction between writer schema and reader schema.

---

## Writer Schema and Reader Schema

The writer schema is the schema used when data was written.

The reader schema is the schema used when data is read.

```text
Writer schema
     |
     v
Avro bytes
     |
     v
Reader schema
     |
     v
Application object
```

Avro can compare the writer schema and reader schema and resolve differences when the change is compatible.

For example, an old writer schema might contain:

```json
{
  "type": "record",
  "name": "OrderCreated",
  "fields": [
    { "name": "orderId", "type": "string" },
    { "name": "amount", "type": "double" }
  ]
}
```

A new reader schema might add a field with a default:

```json
{
  "type": "record",
  "name": "OrderCreated",
  "fields": [
    { "name": "orderId", "type": "string" },
    { "name": "amount", "type": "double" },
    { "name": "currency", "type": "string", "default": "USD" }
  ]
}
```

The old message does not contain `currency`.

Because the new reader schema provides a default, Avro can supply `"USD"`.

Fields are resolved by name between the writer schema and reader schema.

This is why changing field names should be treated as a contract change.

---

## Schema Evolution

Schema evolution is the process of changing schemas over time without breaking producers and consumers.

This is important in Kafka because producers and consumers are usually deployed independently.

```text
Producer v1  --->  Kafka topic  --->  Consumer v2
Producer v2  --->  Kafka topic  --->  Consumer v1
```

Compatibility depends on which direction must continue working.

---

## Backward Compatibility

Backward compatibility means a new reader can read old data.

```text
Old producer writes with schema v1
New consumer reads with schema v2
```

A common backward-compatible change is adding a field with a default.

```json
{
  "name": "currency",
  "type": "string",
  "default": "USD"
}
```

Old records do not contain `currency`, but the new reader can use the default value.

---

## Forward Compatibility

Forward compatibility means an old reader can read new data.

```text
New producer writes with schema v2
Old consumer reads with schema v1
```

An old reader may ignore fields it does not know about.

However, removing a field can break forward compatibility if old readers still expect that field and do not have a default.

---

## Full Compatibility

Full compatibility means both directions work.

```text
New readers can read old data.
Old readers can read new data.
```

This is often desirable for shared Kafka events because producers and consumers may not be deployed at the same time.

---

## Adding Fields

Adding a field should usually include a default.

```json
{
  "name": "currency",
  "type": "string",
  "default": "USD"
}
```

Without a default, new readers may not be able to read old records.

Nullable fields are commonly added like this:

```json
{
  "name": "couponCode",
  "type": ["null", "string"],
  "default": null
}
```

---

## Removing Fields

Removing a field can be safe for new readers if they no longer need the field.

However, old readers may fail when reading new records if they still expect the removed field and do not have a default.

Removal should therefore be treated as a migration.

Consumers should be updated first, and old fields should only be removed once they are no longer required.

---

## Renaming Fields

Renaming a field is dangerous.

To humans, this may look like a rename:

```json
{
  "name": "customerId",
  "type": "string"
}
```

becoming:

```json
{
  "name": "buyerId",
  "type": "string"
}
```

To Avro, this may look like one field was removed and another field was added.

Aliases can help readers match old names to new names, but field renames should still be handled carefully.

A safer migration is:

1. Add the new field.
2. Write both fields temporarily.
3. Migrate consumers.
4. Remove the old field later when it is safe.

---

## Changing Field Types

Changing a field type can break compatibility.

```json
{
  "name": "amount",
  "type": "double"
}
```

to:

```json
{
  "name": "amount",
  "type": "string"
}
```

changes the event contract.

Avro supports some type promotions, but event schemas should avoid casual type changes.

If a field needs a different meaning or representation, it is often safer to add a new field and migrate gradually.

---

## Common Mistakes

### Treating Avro as just faster JSON

Avro is not only about compact payloads.

Its main value in Kafka systems is the explicit schema contract and support for controlled evolution.

### Adding fields without defaults

New readers may not be able to read old records if the new field has no default.

### Assuming nullable fields are automatic

Avro does not allow `null` unless `null` is part of the field type.

### Renaming fields casually

Avro resolves fields by name.

Renaming a field can behave like removing one field and adding another.

### Using enums for values that change often

Enums are useful for stable value sets.

If values change frequently or come from external systems, a string may be safer.

### Overusing maps for important fields

Maps are useful for flexible metadata.

If a field is important to consumers, define it explicitly instead of hiding it inside a map.

### Creating deeply nested event schemas

Nested records can make related data clearer.

Too much nesting can make schemas harder to evolve and consumers harder to write.

### Using double for exact money values

Floating-point values are not ideal when exact decimal precision matters.

Use an Avro decimal logical type for money when exact precision is required.

---

## Interview Corner

### What problem does Avro solve?

Avro provides schema-based serialization.

In Kafka systems, it helps producers and consumers share an explicit event contract that can evolve over time.

### Is Avro the same as JSON?

No.

Avro schemas are written as JSON, but Avro data is commonly serialized as compact binary.

### Why does Avro binary data need a schema?

The binary data contains encoded values.

The schema explains how those values should be interpreted, including field names, types, and record structure.

### What is the difference between writer schema and reader schema?

The writer schema is used when data is written.

The reader schema is used when data is read.

Avro can resolve differences between them when the schemas are compatible.

### Why are defaults important in Avro?

Defaults allow readers to supply values for fields that are missing from the writer schema.

They are especially important when adding new fields.

### What is the difference between primitive and complex Avro types?

Primitive types represent simple values such as strings, numbers, booleans, bytes, and null.

Complex types describe structured values such as records, enums, arrays, maps, unions, and fixed-size binary values.

### When should you use a nested record?

Use a nested record when a group of fields clearly belongs together as part of the event contract.

Avoid nesting only because the internal domain model is nested.

### What are logical types?

Logical types add semantic meaning to an underlying Avro type.

For example, `timestamp-millis` gives meaning to a `long`, and `decimal` gives exact decimal meaning to `bytes` or `fixed`.

---

## Rules of Thumb

- Treat Avro schemas as service contracts.
- Prefer explicit schemas over informal JSON conventions for shared Kafka events.
- Use primitive types for simple values.
- Use complex types when the event contract needs structure.
- Add new fields with defaults.
- Use `["null", "type"]` with `default: null` for nullable fields.
- Be careful with enums if values may expand over time.
- Use maps for metadata, not for core business fields.
- Keep nested records purposeful and understandable.
- Use logical types for timestamps, UUIDs, dates, and exact decimals.
- Avoid renaming fields unless you understand aliases and compatibility impact.
- Avoid changing field meanings without a migration plan.
- Remember that Kafka stores bytes; Avro and Schema Registry provide the contract layer.
