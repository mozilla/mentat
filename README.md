# Project Mentat

Project Mentat is a persistent, embedded knowledge base. It draws heavily on [DataScript](https://github.com/tonsky/datascript) and [Datomic](http://datomic.com).

Mentat is implemented in Rust.

The first version of Project Mentat, named Datomish, [was written in ClojureScript](https://github.com/mozilla/mentat/tree/clojure), targeting both Node (on top of `promise_sqlite`) and Firefox (on top of `Sqlite.jsm`). It also worked in pure Clojure on the JVM on top of `jdbc-sqlite`. The name was changed to avoid confusion with [Datomic](http://datomic.com).

The rust implementation gives us a smaller compiled output, better performance, more type safety, better tooling, and easier deployment into Firefox and mobile platforms.


## Motivation

Mentat is intended to be a flexible relational (not key-value, not document-oriented) store that doesn't leak its storage schema to users, and doesn't make it hard to grow its domain schema and run arbitrary queries.

Our short-term goal for Project Mentat is to build a system that, as the basis for a User Agent Service, can support multiple [Tofino](https://github.com/mozilla/tofino) UX experiments without having a storage engineer do significant data migration, schema work, or revving of special-purpose endpoints.

By abstracting away the storage schema, and by exposing change listeners outside the database (not via triggers), we hope to allow both the data store itself and embedding applications to use better architectures, meeting performance goals in a way that allows future evolution.

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

DataScript asks the question: "What if creating a database would be as cheap as creating a Hashmap?"

Mentat is not interested in that. Instead, it's strongly interested in persistence and performance, with very little interest in immutable databases/databases as values or throwaway use.

One might say that Mentat's question is: "What if an SQLite database could store arbitrary relations, for arbitrary consumers, without them having to coordinate an up-front storage-level schema?"

(Note that [domain-level schemas are very valuable](http://martinfowler.com/articles/schemaless/).)

Another possible question would be: "What if we could bake some of the concepts of CQRS and event sourcing into a persistent relational store, such that the transaction log itself were of value to queries?"

Some thought has been given to how databases as values — long-term references to a snapshot of the store at an instant in time — could work in this model. It's not impossible; it simply has different performance characteristics.

Just like DataScript, Mentat speaks Datalog for querying and takes additions and retractions as input to a transaction. Unlike DataScript, Mentat's API is asynchronous.

Unlike DataScript, Mentat exposes free-text indexing, thanks to SQLite.


## Comparison to Datomic

Datomic is a server-side, enterprise-grade data storage system. Datomic has a beautiful conceptual model. It's intended to be backed by a storage cluster, in which it keeps index chunks forever. Index chunks are replicated to peers, allowing it to run queries at the edges. Writes are serialized through a transactor.

Many of these design decisions are inapplicable to deployed desktop software; indeed, the use of multiple JVM processes makes Datomic's use in a small desktop app, or a mobile device, prohibitive.

Mentat is designed for embedding, initially in an Electron app ([Tofino](https://github.com/mozilla/tofino)). It is less concerned with exposing consistent database states outside transaction boundaries, because that's less important here, and dropping some of these requirements allows us to leverage SQLite itself.


## Comparison to SQLite

SQLite is a traditional SQL database in most respects: schemas conflate semantic, structural, and datatype concerns, as described above; the main interface with the database is human-first textual queries; sparse and graph-structured data are 'unnatural', if not always inefficient; experimenting with and evolving data models are error-prone and complicated activities; and so on.

Mentat aims to offer many of the advantages of SQLite — single-file use, embeddability, and good performance — while building a more relaxed and expressive data model on top.

## Contributing

Please note that this project is released with a Contributor Code of Conduct.
By participating in this project you agree to abide by its terms.

See [CONTRIBUTING.md](/CONTRIBUTING.md) for further notes.

This project is very new, so we'll probably revise these guidelines. Please
comment on an issue before putting significant effort in if you'd like to
contribute.

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

# Run tests for just the query-parser folder (specify the crate, not the folder),
# printing debug output.
cargo test -p mentat_query_parser -- --nocapture
````

For most `cargo` commands you can pass the `-p` argument to run the command just on that package. So, `cargo build -p mentat_query_parser` will build just the "query-parser" folder.

## License

Project Mentat is currently licensed under the Apache License v2.0. See the `LICENSE` file for details.


## SQLite dependencies

Mentat uses partial indices, which are available in SQLite 3.8.0 and higher.

It also uses FTS4, which is [a compile time option](http://www.sqlite.org/fts3.html#section_2).
