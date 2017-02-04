This crate turns a parsed query, as defined by the `query` crate and produced by the `query-parser` crate, into an *algebrized tree*, also called a *query processor tree*.

This is something of a wooly definition: a query algebrizer in a traditional relational database is the component that combines the schema — including column type constraints — with the query, resolving names and that sort of thing. Much of that work is unnecessary in our model; for example, we don't need to resolve column aliases, deal with table names, or that sort of thing. But the similarity is strong enough to give us the name of this crate.

The result of this process is traditionally handed to the *query optimizer* to yield an *execution plan*. In our case the execution plan is deterministically derived from the algebrized tree, and the real optimization (such as it is) takes place within the underlying SQLite database.
