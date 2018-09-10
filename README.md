# UNMAINTAINED Project Mentat
[![Build Status](https://travis-ci.org/mozilla/mentat.svg?branch=master)](https://travis-ci.org/mozilla/mentat)

**Project Mentat is [no longer being developed or actively maintained by Mozilla](https://mail.mozilla.org/pipermail/firefox-dev/2018-September/006780.html).**  This repository will be marked read-only in the near future.  You are, of course, welcome to fork the repository and use the existing code.

Project Mentat is a persistent, embedded knowledge base. It draws heavily on [DataScript](https://github.com/tonsky/datascript) and [Datomic](http://datomic.com).

Mentat is implemented in Rust.

The first version of Project Mentat, named Datomish, [was written in ClojureScript](https://github.com/mozilla/mentat/tree/clojure), targeting both Node (on top of `promise_sqlite`) and Firefox (on top of `Sqlite.jsm`). It also worked in pure Clojure on the JVM on top of `jdbc-sqlite`. The name was changed to avoid confusion with [Datomic](http://datomic.com).

The Rust implementation gives us a smaller compiled output, better performance, more type safety, better tooling, and easier deployment into Firefox and mobile platforms.

[Documentation](https://mozilla.github.io/mentat)

---

## Motivation

Mentat is intended to be a flexible relational (not key-value, not document-oriented) store that makes it easy to describe, grow, and reuse your domain schema.

By abstracting away the storage schema, and by exposing change listeners outside the database (not via triggers), we hope to make domain schemas stable, and allow both the data store itself and embedding applications to use better architectures, meeting performance goals in a way that allows future evolution.

## Data storage is hard

We've observed that data storage is a particular area of difficulty for software development teams:

- It's hard to define storage schemas well. A developer must:
  - Model their domain entities and relationships.
  - Encode that model _efficiently_ and _correctly_ using the features available in the database.
  - Plan for future extensions and performance tuning.

  In a SQL database, the same schema definition defines everything from high-level domain relationships through to numeric field sizes in the same smear of keywords. It's difficult for someone unfamiliar with the domain to determine from such a schema what's a domain fact and what's an implementation concession — are all part numbers always 16 characters long, or are we trying to save space? — or, indeed, whether a missing constraint is deliberate or a bug.

  The developer must think about foreign key constraints, compound uniqueness, and nullability. They must consider indexing, synchronizing, and stable identifiers. Most developers simply don't do enough work in SQL to get all of these things right. Storage thus becomes the specialty of a few individuals.

   Which one of these is correct?

   ```edn
   {:db/id          :person/email
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/many     ; People can have multiple email addresses.
    :db/unique      :db.unique/identity      ; For our purposes, each email identifies one person.
    :db/index       true}                    ; We want fast lookups by email.
   {:db/id          :person/friend
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/many}    ; People can have many friends.
   ```
   ```sql
   CREATE TABLE people (
     id INTEGER PRIMARY KEY,  -- Bug: because of the primary key, each person can have no more than 1 email.
     email VARCHAR(64),       -- Bug?: no NOT NULL, so a person can have no email.
                              -- Bug: nobody will ever have a long email address, right?
   );
   CREATE TABLE friendships (
     FOREIGN KEY person REFERENCES people(id),  -- Bug?: no indexing, so lookups by friend or person will be slow.
     FOREIGN KEY friend REFERENCES people(id),  -- Bug: no compound uniqueness constraint, so we can have dupe friendships.
   );
   ```

   They both have limitations — the Mentat schema allows only for an open world (it's possible to declare friendships with people whose email isn't known), and requires validation code to enforce email string correctness — but we think that even such a tiny SQL example is harder to understand and obscures important domain decisions.

- Queries are intimately tied to structural storage choices. That not only hides the declarative domain-level meaning of the query — it's hard to tell what a query is trying to do when it's a 100-line mess of subqueries and `LEFT OUTER JOIN`s — but it also means a simple structural schema change requires auditing _every query_ for correctness.

- Developers often capture less event-shaped than they perhaps should, simply because their initial requirements don't warrant it. It's quite common to later want to [know when a fact was recorded](https://bugzilla.mozilla.org/show_bug.cgi?id=1341939), or _in which order_ two facts were recorded (particularly for migrations), or on which device an event took place… or even that a fact was _ever_ recorded and then deleted.

- Common queries are hard. Storing values only once, upserts, complicated joins, and group-wise maxima are all difficult for non-expert developers to get right.

- It's hard to evolve storage schemas. Writing a robust SQL schema migration is hard, particularly if a bad migration has ever escaped into the wild! Teams learn to fear and avoid schema changes, and eventually they ship a table called `metadata`, with three `TEXT` columns, so they never have to write a migration again. That decision pushes storage complexity into application code. (Or they start storing unversioned JSON blobs in the database…)

- It's hard to share storage with another component, let alone share _data_ with another component. Conway's Law applies: your software system will often grow to have one database per team.

- It's hard to build efficient storage and querying architectures. Materialized views require knowledge of triggers, or the implementation of bottleneck APIs. _Ad hoc_ caches are often wrong, are almost never formally designed (do you want a write-back, write-through, or write-around cache? Do you know the difference?), and often aren't reusable. The average developer, faced with a SQL database, has little choice but to build a simple table that tries to meet every need.


## Comparison to DataScript

DataScript asks the question: "What if creating a database were as cheap as creating a Hashmap?"

Mentat is not interested in that. Instead, it's strongly interested in persistence and performance, with very little interest in immutable databases/databases as values or throwaway use.

One might say that Mentat's question is: "What if an SQLite database could store arbitrary relations, for arbitrary consumers, without them having to coordinate an up-front storage-level schema?"

(Note that [domain-level schemas are very valuable](http://martinfowler.com/articles/schemaless/).)

Another possible question would be: "What if we could bake some of the concepts of [CQRS and event sourcing](http://www.baeldung.com/cqrs-event-sourced-architecture-resources) into a persistent relational store, such that the transaction log itself were of value to queries?"

Some thought has been given to how databases as values — long-term references to a snapshot of the store at an instant in time — could work in this model. It's not impossible; it simply has different performance characteristics.

Just like DataScript, Mentat speaks Datalog for querying and takes additions and retractions as input to a transaction.

Unlike DataScript, Mentat exposes free-text indexing, thanks to SQLite.


## Comparison to Datomic

Datomic is a server-side, enterprise-grade data storage system. Datomic has a beautiful conceptual model. It's intended to be backed by a storage cluster, in which it keeps index chunks forever. Index chunks are replicated to peers, allowing it to run queries at the edges. Writes are serialized through a transactor.

Many of these design decisions are inapplicable to deployed desktop software; indeed, the use of multiple JVM processes makes Datomic's use in a small desktop app, or a mobile device, prohibitive.

Mentat was designed for embedding, initially in an experimental Electron app ([Tofino](https://github.com/mozilla/tofino)). It is less concerned with exposing consistent database states outside transaction boundaries, because that's less important here, and dropping some of these requirements allows us to leverage SQLite itself.


## Comparison to SQLite

SQLite is a traditional SQL database in most respects: schemas conflate semantic, structural, and datatype concerns, as described above; the main interface with the database is human-first textual queries; sparse and graph-structured data are 'unnatural', if not always inefficient; experimenting with and evolving data models are error-prone and complicated activities; and so on.

Mentat aims to offer many of the advantages of SQLite — single-file use, embeddability, and good performance — while building a more relaxed, reusable, and expressive data model on top.

---

## Contributing

Please note that this project is released with a Contributor Code of Conduct.
By participating in this project you agree to abide by its terms.

See [CONTRIBUTING.md](CONTRIBUTING.md) for further notes.

This project is very new, so we'll probably revise these guidelines. Please
comment on an issue before putting significant effort in if you'd like to
contribute.

---

## Building

You first need to clone the project.  To build and test the project, we are using [Cargo](https://crates.io/install).

To build all of the crates in the project use:

````
cargo build
````

To run tests use:

````
# Run tests for everything.
cargo test --all

# Run tests for just the query-algebrizer folder (specify the crate, not the folder),
# printing debug output.
cargo test -p mentat_query_algebrizer -- --nocapture
````

For most `cargo` commands you can pass the `-p` argument to run the command just on that package. So, `cargo build -p mentat_query_algebrizer` will build just the "query-algebrizer" folder.

## What are all of these crates?

We use multiple sub-crates for Mentat for four reasons:

1. To improve incremental build times.
2. To encourage encapsulation; writing `extern crate` feels worse than just `use mod`.
3. To simplify the creation of targets that don't use certain features: _e.g._, a build with no syncing, or with no query system.
4. To allow for reuse (_e.g._, the EDN parser is essentially a separate library).

So what are they?

### Building blocks

#### `edn`

Our EDN parser. It uses `rust-peg` to parse [EDN](https://github.com/edn-format/edn), which is Clojure/Datomic's richer alternative to JSON. `edn`'s dependencies are all either for representing rich values (`chrono`, `uuid`, `ordered-float`) or for parsing (`serde`, `peg`).

In addition, this crate turns a stream of EDN values into a representation suitable to be transacted.

#### `mentat_core`

This is the lowest-level Mentat crate. It collects together the following things:

- Fundamental domain-specific data structures like `ValueType` and `TypedValue`.
- Fundamental SQL-related linkages like `SQLValueType`. These encode the mapping between Mentat's types and values and their representation in our SQLite format.
- Conversion to and from EDN types (_e.g._, `edn::Keyword` to `TypedValue::Keyword`).
- Common utilities (some in the `util` module, and others that should be moved there or broken out) like `Either`, `InternSet`, and `RcCounter`.
- Reusable lazy namespaced keywords (_e.g._, `DB_TYPE_DOUBLE`) that are used by `mentat_db` and EDN serialization of core structs.

### Types

#### `mentat_query`

This crate defines the structs and enums that are the output of the query parser and used by the translator and algebrizer. `SrcVar`, `NonIntegerConstant`, `FnArg`… these all live here.

#### `mentat_query_sql`

Similarly, this crate defines an abstract representation of a SQL query as understood by Mentat. This bridges between Mentat's types (_e.g._, `TypedValue`) and SQL concepts (`ColumnOrExpression`, `GroupBy`). It's produced by the algebrizer and consumed by the translator.

### Query processing

#### `mentat_query_algebrizer`

This is the biggest piece of the query engine. It takes a parsed query, which at this point is _independent of a database_, and combines it with the current state of the schema and data. This involves translating keywords into attributes, abstract values into concrete values with a known type, and producing an `AlgebraicQuery`, which is a representation of how a query's Datalog semantics can be satisfied as SQL table joins and constraints over Mentat's SQL schema. An algebrized query is tightly coupled with both the disk schema and the vocabulary present in the store when the work is done.

#### `mentat_query_projector`

A Datalog query _projects_ some of the variables in the query into data structures in the output. This crate takes an algebrized query and a projection list and figures out how to get values out of the running SQL query and into the right format for the consumer.

#### `mentat_query_translator`

This crate works with all of the above to turn the output of the algebrizer and projector into the data structures defined in `mentat_query_sql`.

#### `mentat_sql`

This simple crate turns those data structures into SQL text and bindings that can later be executed by `rusqlite`.

### The data layer: `mentat_db`

This is a big one: it implements the core storage logic on top of SQLite. This crate is responsible for bootstrapping new databases, transacting new data, maintaining the attribute cache, and building and updating in-memory representations of the storage schema.

### The main crate

The top-level main crate of Mentat assembles these component crates into something useful. It wraps up a connection to a database file and the associated metadata into a `Store`, and encapsulates an in-progress transaction (`InProgress`). It provides modules for programmatically writing (`entity_builder.rs`) and managing vocabulary (`vocabulary.rs`).

### Syncing

Sync code lives, for [referential reasons](https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying), in a crate named `tolstoy`. This code is a work in progress; current state is a proof-of-concept implementation which largely relies on the internal transactor to make progress in most cases and comes with a basic support for timelines. See [Tolstoy's documentation](https://github.com/mozilla/mentat/tree/master/tolstoy/README.md) for details.

### The command-line interface

This is under `tools/cli`. It's essentially an external consumer of the main `mentat` crate. This code is ugly, but it mostly works.

---

## SQLite dependencies

Mentat uses partial indices, which are available in SQLite 3.8.0 and higher. It relies on correlation between aggregate and non-aggregate columns in the output, which was added in SQLite 3.7.11.

It also uses FTS4, which is [a compile time option](http://www.sqlite.org/fts3.html#section_2).

By default, Mentat specifies the `"bundled"` feature for `rusqlite`, which uses a relatively recent
version of SQLite. If you want to link against the system version of SQLite, omit `"bundled_sqlite3"`
from Mentat's features.

```toml
[dependencies.mentat]
version = "0.6"
# System sqlite is known to be new.
default-features = false
```

---

## License

Project Mentat is currently licensed under the Apache License v2.0. See the `LICENSE` file for details.
