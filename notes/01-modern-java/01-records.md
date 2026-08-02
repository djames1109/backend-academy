# Java Records

## Introduction

Records were introduced to simplify classes whose primary purpose is to carry data.

Instead of writing constructors, getters, `equals()`, `hashCode()`, and `toString()` repeatedly, Java can generate them automatically.

More importantly, records communicate intent. They tell other developers that this type represents immutable data rather than mutable state or business behavior.

## Why Records Exist

Before records, Java developers wrote countless classes whose only purpose was to transport data.

These classes often contained little more than fields, constructors, getters, and implementations of `equals()`, `hashCode()`, and `toString()`.

This repetitive code became known as *boilerplate*.

Records reduce that boilerplate while making the intent of the type explicit.

A record tells both the compiler and other developers:

> This object represents immutable data.

Instead of focusing on how the class is implemented, records let you focus on what data the class represents.

---

## The Problem

Consider a typical DTO.

```java
public class User {

    private final String name;
    private final String email;

    public User(String name, String email) {
        this.name = name;
        this.email = email;
    }

    public String getName() {
        return name;
    }

    public String getEmail() {
        return email;
    }

    @Override
    public boolean equals(Object o) {
        ...
    }

    @Override
    public int hashCode() {
        ...
    }

    @Override
    public String toString() {
        ...
    }
}
```

Most of the class is boilerplate.

Now compare it with a record.

```java
public record User(
    String name,
    String email
) {}
```

Java automatically generates:

- Canonical constructor (a constructor that accepts all parameters in the same order as they are declared)
- Accessor methods
- `equals()`
- `hashCode()`
- `toString()`

---

## Accessor Methods

Unlike normal classes, records do not generate JavaBean getters.

```java
User user = new User("John", "john@email.com");

user.name();
user.email();
```

instead of

```java
user.getName();
```

The accessor method has the same name as the record component.

---

## Immutability

Records are immutable.

Once constructed, their fields cannot be reassigned.

```java
public record User(
    String name,
    String email
) {}
```

The following is impossible.

```java
user.name = "Jane";
```

There are no setters either.

This makes records ideal for request objects, response objects, events, and value objects.

---

## Shallow vs Deep Immutability

One common misconception is that records make everything immutable.

They don't.

Consider the following.

```java
public record Customer(
    String name,
    List<String> accounts
) {}
```

```java
Customer customer = new Customer(
    "John",
    new ArrayList<>(List.of("Savings"))
);

customer.accounts().add("Checking");
```

This compiles and runs successfully.

The record itself is immutable because the reference cannot change.

The `ArrayList` is still mutable.

This is known as **shallow immutability**.

---

## Defensive Copying

If a record stores mutable objects, make defensive copies.

```java
public record Customer(
    String name,
    List<String> accounts
) {

    public Customer {
        accounts = List.copyOf(accounts);
    }
}
```

Now the record owns its own immutable copy.

Future changes to the original list do not affect the record.

---

## List.copyOf() vs Collections.unmodifiableList()

These methods are often confused.

### List.copyOf()

- Creates a new immutable list.
- Changes to the original list are not reflected.
- Rejects `null` elements.

```java
List<String> original = new ArrayList<>(List.of("A"));

List<String> copy = List.copyOf(original);

original.add("B");

System.out.println(copy); // [A]
```

### Collections.unmodifiableList()

- Creates an unmodifiable view.
- The original list is still mutable.
- Changes to the original list are reflected.

```java
List<String> original = new ArrayList<>(List.of("A"));

List<String> view = Collections.unmodifiableList(original);

original.add("B");

System.out.println(view); // [A, B]
```

For records, `List.copyOf()` is generally the better choice.

---

## Compact Constructors

Records support constructors for validation and normalization.

```java
public record User(
    String name,
    String email
) {

    public User {
        Objects.requireNonNull(name);

        email = email.toLowerCase();
    }
}
```

The fields are assigned automatically after the constructor completes.

Use compact constructors for:

- Validation
- Normalization
- Defensive copying

---

## Records Can Have Methods

Records are not limited to fields.

```java
public record Money(BigDecimal amount) {

    public boolean isNegative() {
        return amount.signum() < 0;
    }
}
```

Methods are useful when they operate directly on the record's data.

---

## When to Use Records

Records work well for immutable data.

Examples:

- Request DTOs
- Response DTOs
- API payloads
- Events
- Value Objects
- Query Projections
- Configuration Objects

Rule of thumb:

> If the object only carries data, consider using a record.

---

## When Not to Use Records

Prefer a normal class when:

- The object has mutable state.
- Values change over time.
- The object contains rich business logic.
- The object is a JPA entity.

For example, an `Account` class with methods like `deposit()`, `withdraw()`, and `freeze()` should remain a normal class.

---

## Common Mistakes

### Assuming records are deeply immutable

Collections and other mutable objects can still change unless you defensively copy them.

### Using records as JPA entities

JPA expects mutable objects, proxying, and lifecycle management.

### Using records everywhere

Not every class is a data carrier.

Choose records because they communicate intent, not because they are shorter.

---

## Interview Corner

### Are records immutable?

Records are **shallowly immutable**.

The fields are final, so their references cannot change after construction.

If a field references a mutable object, that object's state can still change unless it is defensively copied.

---

## Did You Know?

- Records implicitly extend `java.lang.Record`.
- Records are implicitly `final`.
- Records cannot extend another class.
- Records can implement interfaces.
- Accessor methods are named after the components (`name()`), not JavaBean getters (`getName()`).

---

## Rules of Thumb

- Use records for immutable data carriers.
- Prefer records for DTOs, events, and value objects.
- Defensively copy mutable collections.
- Prefer `List.copyOf()` over `Collections.unmodifiableList()` for records.
- Keep JPA entities as normal classes.