This crate turns an algebrized Datalog query into a domain-specific representation of a SQL query, and then uses `mentat_sql` to turn that into a SQL string to be executed.

This subsumes both planning and query construction, because in Mentat the SQL query is effectively a query plan.
