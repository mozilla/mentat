# Datomish

Datomish is a persistent, embedded knowledge base. It's written in ClojureScript, and draws heavily on [DataScript](https://github.com/tonsky/datascript) and [Datomic](http://datomic.com).

Datomish compiles into a single JavaScript file, and is usable both in Node (on top of `promise_sqlite`) and in Firefox (on top of `Sqlite.jsm`). It also works in pure Clojure on the JVM on top of `jdbc-sqlite`.

There's an example Firefox restartless add-on in the [`addon`](https://github.com/mozilla/datomish/tree/master/addon) directory; build instructions are below.


## Motivation

Datomish is intended to be a flexible relational (not key-value, not document-oriented) store that doesn't leak its storage schema to users, and doesn't make it hard to grow its domain schema and run arbitrary queries.

Our short-term goal is to build a system that, as the basis for a User Agent Service, can support multiple [Tofino](https://github.com/mozilla/tofino) UX experiments without having a storage engineer do significant data migration, schema work, or revving of special-purpose endpoints.


## Comparison to DataScript

DataScript asks the question: "What if creating a database would be as cheap as creating a Hashmap?"

Datomish is not interested in that. Instead, it's strongly interested in persistence and performance, with very little interest in immutable databases/databases as values or throwaway use.

One might say that Datomish's question is: "What if an SQLite database could store arbitrary relations, for arbitrary consumers, without them having to coordinate an up-front storage-level schema?"

(Note that [domain-level schemas are very valuable](http://martinfowler.com/articles/schemaless/).)

Another possible question would be: "What if we could bake some of the concepts of CQRS and event sourcing into a persistent relational store, such that the transaction log itself were of value to queries?"

Some thought has been given to how databases as values — long-term references to a snapshot of the store at an instant in time — could work in this model. It's not impossible; it simply has different performance characteristics.

Just like DataScript, Datomish speaks Datalog for querying and takes additions and retractions as input to a transaction. Unlike DataScript, Datomish's API is asynchronous.

Unlike DataScript, Datomish exposes free-text indexing, thanks to SQLite.


## Comparison to Datomic

Datomic is a server-side, enterprise-grade data storage system. Datomic has a beautiful conceptual model. It's intended to be backed by a storage cluster, in which it keeps index chunks forever. Index chunks are replicated to peers, allowing it to run queries at the edges. Writes are serialized through a transactor.

Many of these design decisions are inapplicable to deployed desktop software; indeed, the use of multiple JVM processes makes Datomic's use in a small desktop app, or a mobile device, prohibitive.

Datomish is designed for embedding, initially in an Electron app ([Tofino](https://github.com/mozilla/tofino)). It is less concerned with exposing consistent database states outside transaction boundaries, because that's less important here, and dropping some of these requirements allows us to leverage SQLite itself.


## Comparison to SQLite

SQLite is a traditional SQL database in most respects: schemas conflate semantic, structural, and datatype concerns; the main interface with the database is human-first textual queries; sparse and graph-structured data are 'unnatural', if not always inefficient; experimenting with and evolving data models are error-prone and complicated activities; and so on.

Datomish aims to offer many of the advantages of SQLite — single-file use, embeddability, and good performance — while building a more relaxed and expressive data model on top.


## Contributing

Please note that this project is released with a Contributor Code of Conduct.
By participating in this project you agree to abide by its terms.

See [CONTRIBUTING.md](/CONTRIBUTING.md) for further notes.

This project is very new, so we'll probably revise these guidelines. Please
comment on a bug before putting significant effort in if you'd like to
contribute.


## License

At present this code is licensed under MPLv2.0. That license is subject to change prior to external contributions.

Datomish includes some code from DataScript, kindly relicensed by Nikita Prokopov.


## SQLite dependencies

Datomish uses partial indices, which are available in SQLite 3.8.0 and higher.

It also uses FTS4, which is [a compile time option](http://www.sqlite.org/fts3.html#section_2).


## Prep

You'll need [Leiningen](http://leiningen.org).

```
# If you use nvm.
nvm use 6

lein deps
npm install

# If you want a decent REPL.
brew install rlwrap
```

## Running a REPL

### Starting a ClojureScript REPL from the terminal

```
rlwrap lein run -m clojure.main repl.clj
```

### Connecting to a ClojureScript environment from Vim

You'll need `vim-fireplace`. Install using Pathogen.

First, start a Clojure REPL with an nREPL server. Then load our ClojureScript REPL and dependencies. Finally, connect to it from Vim.

```
$ lein repl
nREPL server started on port 62385 on host 127.0.0.1 - nrepl://127.0.0.1:62385
REPL-y 0.3.7, nREPL 0.2.10
Clojure 1.8.0
Java HotSpot(TM) 64-Bit Server VM 1.8.0_60-b27
    Docs: (doc function-name-here)
          (find-doc "part-of-name-here")
  Source: (source function-name-here)
 Javadoc: (javadoc java-object-or-class-here)
    Exit: Control+D or (exit) or (quit)
 Results: Stored in vars *1, *2, *3, an exception in *e

user=> (load-file "repl.clj")
Reading analysis cache for jar:file:/Users/rnewman/.m2/repository/org/clojure/clojurescript/1.9.89/clojurescript-1.9.89.jar!/cljs/core.cljs
Compiling out/cljs/nodejs.cljs
Compiling src/datomish/sqlite.cljs
Compiling src/datomish/core.cljs
ClojureScript Node.js REPL server listening on 57134
Watch compilation log available at: out/watch.log
To quit, type: :cljs/quit
cljs.user=>
```

in Vim, in the working directory:

```
:Piggieback (cljs.repl.node/repl-env)
```

Now you can use `:Eval`, `cqc`, and friends to evaluate code. Fireplace should connect automatically, but if it doesn't:

```
:Connect nrepl://localhost:62385
```

## To run tests in ClojureScript

Run `lein doo node test once`, or `lein doo node` to re-run on file changes.

## To run tests in Clojure

Run `lein test`.

## To run smoketests with the built release library in a Node environment

```
# Build.
lein cljsbuild once release-node
npm run test
```

## To build for Firefox

```
lein cljsbuild once release-browser
```

### To build and run the example Firefox add-on:

First build for Firefox, so that `datomish.js` exists.

Then:

```
cd addon
./build.sh
cd release
./run.sh
```

## Preparing an NPM release

The intention is that the `target/release-node/` directory is roughly the shape of an npm-ready JavaScript package.

To generate a require/import-ready `target/release-node/datomish.js`, run
```
lein cljsbuild once release-node
```
To verify that importing into Node.js succeeds, run
```
npm run test
```

## To locally install for ClojureScript use

```
lein with-profile node install
```

Many thanks to ([David Nolen](https://github.com/swannodette)) and ([Nikita Prokopov](https://github.com/tonsky)) for demonstrating how to package ClojureScript for distribution via npm.
