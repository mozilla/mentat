# Introduction

Mentat is a transactional, relational storage system built on top of SQLite. The abstractions it offers allow you to easily tackle some things that are tricky in other storage systems:

- Have multiple components share storage and collaborate.
- Evolve schema.
- Track change over time.
- Synchronize data correctly.
- Store data with rich, checked types.

Mentat offers a programmatic Rust API for managing stores, retrieving data, and _transacting_ new data. It offers a Datalog-based query engine, with queries expressed in EDN, a rich textual data format similar to JSON. And it offers an EDN data format for transacting new data.

This tutorial covers all of these APIs, along with defining vocabulary.

We'll begin by introducing some concepts, and then we'll walk through some examples.


## What does Mentat store?

Depending on your perspective, Mentat looks like a relational store, a graph store, or a tuple store.

Mentat stores relationships between _entities_ and other entities or _values_. An entity is related to other things by an _attribute_.

All entities have an _entity ID_ (abbreviated to _entid_).

Some entities additionally have an identifier called an _ident_, which is a keyword. That looks something like `:bookmark/title`.

A value is a primitive piece of data. Mentat supports the following:

* Strings
* Long integers
* Double-precision floating point numbers
* Millisecond-precision timestamps
* UUIDs
* Booleans
* Keywords (a special kind of string that we use for idents).

There are two special kinds of entities: _attributes_ and _transactions_.

Attributes are themselves entities with a particular set of properties that define their meaning. They have identifiers, so you can refer to them easily. They have a _value type_, which is the type of value Mentat expects to be on the right hand side of the relationship. And they have a _cardinality_ (whether one or many values can exist for a particular entity), whether values are _unique_, a documentation string, and some indexing options.

An attribute looks something like this:

```edn
{:db/ident       :bookmark/title
 :db/cardinality :db.cardinality/one
 :db/valueType   :db.type/string
 :db/fulltext    true
 :db/doc         "The title of a bookmark."}
```

Transactions are special entities that can be described however you wish. By default they track the timestamp at which they were written.

The relationship between an entity, an attribute, and a value, occurring in a _transaction_ (which is just another kind of entity!) — a tuple of five values — is called a _datom_.

A single datom might look something like this:

```
       [:db/add          65536         :bookmark/title       "mozilla.org"              268435456]
        ^                  ^              ^                     ^                         ^
        \ Add or retract.  |              |                     |                         |
                           \ The entity.  |                     |                         |
                                          \ The attribute.      |                         |
                                                                \ The value, a string.    |
                                                                                          \ The transaction ID.
```

which is equivalent to saying "in transaction 268435456 we assert that entity 65536 is a bookmark with the title 'mozilla.org'".

When we transact that — which means to add it as a fact to the store — Mentat also describes the transaction itself on our behalf:

```edn
       [:db/add 268435456 :db/txInstant "2018-01-25 20:07:04.408358 UTC" 268435456]
```

# A simple app

Let's get started with some Rust code.

First, the imports we'll need. The comments here briefly explain what each thing is.

```rust
// So you can define keywords with neater syntax.
#[macro_use(kw)]
extern crate mentat;

use mentat::{
    Store,          // A single database connection and in-memory metadata.
}

use mentat::vocabulary::attribute;      // Properties of attributes.
```

## Defining a simple vocabulary

All data in Mentat — even the terms we used above, like `:db/cardinality` — are defined in the store itself. So that's where we start. In Rust, we define a _vocabulary definition_, and ask the store to ensure that it exists.

```rust

fn set_up(mut store: Store) -> Result<()> {
    // Start a write transaction.
    let mut in_progress = store.begin_transaction()?;

    // Make sure the core vocabulary exists. This is good practice if a user,
    // an external tool, or another component might have touched the file
    // since you last wrote it.
    in_progress.verify_core_schema()?;

    // Make sure our vocabulary is installed, and install if necessary.
    // This defines some attributes that we can use to describe people.
    in_progress.ensure_vocabulary(&Definition {
        name: kw!(:example/people),
        version: 1,
        attributes: vec![
            (kw!(:person/name),
             vocabulary::AttributeBuilder::default()
               .value_type(ValueType::String)
               .multival(true)
               .build()),
            (kw!(:person/age),
             vocabulary::AttributeBuilder::default()
               .value_type(ValueType::Long)
               .multival(false)
               .build()),
            (kw!(:person/email),
             vocabulary::AttributeBuilder::default()
               .value_type(ValueType::String)
               .multival(true)
               .unique(attribute::Unique::Identity)
               .build()),
        ],
    })?;

    in_progress.commit()?;
    Ok(())
}
```

We open a store and configure its vocabulary like this:

```rust
let path = "/path/to/file.db";
let store = Store::open(path)?;
set_up(store)?;
```

If this code returns successfully, we're good to go.

## Transactions

You'll see in our `set_up` function that we begin and end a transaction, which we call `in_progress`. A read-only transaction is begun via `begin_read`. The resulting objects — `InProgress` and `InProgressRead` support various kinds of read and write operations. Transactions are automatically rolled back when dropped, so remember to call `commit`!

## Adding some data

There are two ways to add data to Mentat: programmatically or textually.

The textual form accepts EDN, a simple relative of JSON that supports richer types and more flexible syntax. You saw this in the introduction. Here's an example:

```rust
in_progress.transact(r#"[
    {:person/name "Alice"
     :person/age 32
     :person/email "alice@example.org"}
]"#)?;
```

You can implicitly _upsert_ data when you have a unique attribute to use:

```rust
// Alice's age is now 33. Note that we don't need to find out an entid,
// nor explicitly INSERT OR REPLACE or UPDATE OR INSERT or similar.
in_progress.transact(r#"[
    {:person/age 33
     :person/email "alice@example.org"}
]"#)?;
```

