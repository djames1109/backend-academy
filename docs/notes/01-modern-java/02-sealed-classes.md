# Sealed Classes

## Introduction

Sealed classes and interfaces were introduced to give developers explicit control over inheritance.

Before Java 17, an abstract class or interface could generally be extended or implemented by anyone. While this is useful for extension points such as `Runnable` or Spring interfaces, some hierarchies are intended to have only a fixed set of top-level implementations.

Sealed types allow the owner of a type hierarchy to define exactly which classes or interfaces are allowed to extend or implement it.

---

## Why Sealed Classes Exist

Sometimes a type is designed to have only a limited number of valid implementations.

Without sealed types, anyone can create a new subclass or implementation, even if it was never intended by the original design.

Sealed classes make this restriction explicit and enforce it at compile time.

Instead of relying on documentation or conventions, the language itself guarantees that only the permitted types can extend the hierarchy.

---

## The Problem

Suppose you're designing a shared library.

```java
public interface Message {
}
```

Anyone can introduce a new implementation.

```java
public class Notification implements Message {
}

public class Query implements Message {
}
```

If your library only understands commands and events, these additional implementations may be unsupported or violate the intended design.

---

## Sealed Types

A sealed type explicitly defines who may extend or implement it.

```java
public sealed interface Message
    permits Command, Event {
}
```

Now only `Command` and `Event` may implement `Message`.

Attempting to introduce another top-level implementation results in a compilation error.

---

## The `permits` Keyword

The `permits` clause lists every class or interface that is allowed to extend or implement the sealed type.

```java
public sealed interface PaymentProcessor
    permits CreditCardProcessor,
            BankTransferProcessor {
}
```

This creates a closed hierarchy.

```text
PaymentProcessor
├── CreditCardProcessor
└── BankTransferProcessor
```

---

## `final`, `sealed`, and `non-sealed`

Every permitted subtype must explicitly choose one of three modifiers.

### `final`

Stops inheritance completely.

```java
public final class CreditCardProcessor
        implements PaymentProcessor {
}
```

No other class can extend `CreditCardProcessor`.

---

### `sealed`

Continues restricting the hierarchy.

```java
public sealed interface Command
    extends Message
    permits PaymentCommand,
            CustomerCommand {
}
```

The restriction continues to the next level.

---

### `non-sealed`

Reopens the hierarchy.

```java
public non-sealed interface Event
    extends Message {
}
```

Application code may now define additional event types.

```java
public class PaymentCreatedEvent
        implements Event {
}
```

Only the `Event` branch is open again. The `Message` hierarchy remains controlled.

---

## When Should You Use It?

Use a normal interface or abstract class when the goal is **open extension**.

Use a sealed type when the goal is **controlled extension**.

Ask yourself:

> **Should another developer be able to introduce a new top-level implementation without changing my library or domain model?**

- **Yes** → Use a normal interface or abstract class.
- **No** → A sealed hierarchy is likely the better choice.

Sealed types allow the owner of the hierarchy to decide where extension is allowed.

Common examples include:

- Shared libraries and SDKs
- Internal frameworks
- Workflow engines
- Expression trees (ASTs)
- Domain models with a fixed set of top-level implementations

---

## Enterprise Example

Imagine building a reusable Kafka library.

Every message follows a common envelope.

```java
public sealed abstract class Message<B>
        permits Command, Event {
}
```

The library defines two top-level categories.

```text
Message
├── Command
└── Event
```

The branches are then reopened.

```java
public non-sealed abstract class Command<B>
        extends Message<B> {
}

public non-sealed abstract class Event<B>
        extends Message<B> {
}
```

Applications remain free to create their own business-specific messages.

```text
Command
├── CreatePaymentCommand
├── CancelPaymentCommand
└── UpdateCustomerCommand

Event
├── PaymentCreatedEvent
├── PaymentFailedEvent
└── CustomerUpdatedEvent
```

This allows the library to standardize message structure, routing, serialization, and processing while still allowing applications to define their own commands and events.

---

## When Not to Use It

A sealed hierarchy is usually unnecessary when:

- The hierarchy is designed to be implemented by application developers.
- The set of implementations is expected to evolve frequently.
- A normal interface or abstract class already provides the required flexibility.

Examples include:

- `Runnable`
- `Comparator`
- Spring extension points
- Repository interfaces

These are intentionally designed for open extension.

---

## Common Mistakes

### Sealing every interface

Not every hierarchy needs to be closed.

If application developers are expected to provide their own implementations, a normal interface is usually the better choice.

### Confusing sealed with final

A sealed type still supports inheritance.

It simply controls where inheritance is allowed.

### Using sealed types because there are only a few implementations

A small number of implementations today does not necessarily mean the hierarchy should be sealed.

Choose sealed types because you want **controlled extension**, not because the hierarchy currently happens to be small.

---

## Interview Corner

### Why must a permitted subtype declare `final`, `sealed`, or `non-sealed`?

Every permitted subtype must explicitly define what happens to inheritance from that point onward.

- `final` ends the inheritance branch.
- `sealed` keeps the branch restricted.
- `non-sealed` reopens the branch for normal inheritance.

```java
public sealed interface Message
    permits Command, Event {
}

public non-sealed interface Command
    extends Message {
}

public sealed interface Event
    extends Message
    permits PaymentCreated,
            PaymentFailed {
}
```

Java requires this decision to be explicit so that developers and the compiler can understand the inheritance policy of every branch.

This also supports exhaustive pattern matching because the compiler knows all permitted top-level implementations.

---

## Did You Know?

- Sealed classes and interfaces became permanent features in Java 17.
- A permitted class must declare `final`, `sealed`, or `non-sealed`.
- A permitted subinterface must declare `sealed` or `non-sealed`.
- Records are implicitly `final` and can implement sealed interfaces.
- The `permits` clause can be omitted when the permitted types are declared in the same source file.
- Permitted types must be in the same package or, when using named modules, the same module as the sealed type.
- A `non-sealed` subtype reopens only its own branch of the hierarchy.

---

## Rules of Thumb

- Use normal interfaces or abstract classes for open extension.
- Use sealed types for controlled extension.
- Seal a hierarchy only when you own its valid top-level implementations.
- Use `final` to close a branch.
- Use `sealed` to continue controlling a branch.
- Use `non-sealed` to deliberately reopen a branch.
- Do not seal a type merely because it currently has only a few implementations.