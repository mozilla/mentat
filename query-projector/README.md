This module handles the derivation from an algebrized query of two things:

- A SQL projection: a mapping from columns mentioned in the body of the query to columns in the output.
- A Datalog projection: a function that consumes rows of the appropriate shape (as defined by the SQL projection) to yield one of the four kinds of Datalog query result.

These two must naturally coordinate, and so they are both produced here.
