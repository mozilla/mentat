// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code, unused_imports)]

use mentat_query_algebrizer::{
    AlgebraicQuery,
    ConjoiningClauses,
    DatomsColumn,
    DatomsTable,
    QualifiedAlias,
    SourceAlias,
};

use mentat_sql::{
    BuildQueryError,
    BuildQueryResult,
    QueryBuilder,
    QueryFragment,
    SQLiteQueryBuilder,
};

//---------------------------------------------------------
// A Mentat-focused representation of a SQL query.

enum ColumnOrExpression {
    Column(QualifiedAlias),
    Integer(i64),             // Because it's so common.
}

type Name = String;
struct Projection (ColumnOrExpression, Name);

#[derive(Clone)]
struct Op(String);      // TODO
enum Constraint {
    Infix {
        op: Op,
        left: ColumnOrExpression,
        right: ColumnOrExpression
    }
}

enum JoinOp {
    Inner,
}

// Short-hand for a list of tables all inner-joined.
struct TableList(Vec<SourceAlias>);

struct Join {
    left: TableOrSubquery,
    op: JoinOp,
    right: TableOrSubquery,
    // TODO: constraints (ON, USING).
}
enum TableOrSubquery {
    Table(SourceAlias),
    // TODO: Subquery.
}

enum FromClause {
    TableList(TableList),      // Short-hand for a pile of inner joins.
    Join(Join),
}

struct SelectQuery {
    projection: Vec<Projection>,
    from: FromClause,
    constraints: Vec<Constraint>,
}

//---------------------------------------------------------
// Turn that representation into SQL.

impl QueryFragment for ColumnOrExpression {
    fn to_sql(&self, out: &mut QueryBuilder) -> BuildQueryResult {
        use self::ColumnOrExpression::*;
        match self {
            &Column(QualifiedAlias(ref table, ref column)) => {
                out.push_identifier(table.as_str())?;
                out.push_sql(".");
                out.push_identifier(column.as_str())
            },
            &Integer(i) => {
                out.push_sql(i.to_string().as_str());
                Ok(())
            }
        }
    }
}

impl QueryFragment for Projection {
    fn to_sql(&self, out: &mut QueryBuilder) -> BuildQueryResult {
        self.0.to_sql(out)?;
        out.push_sql(" AS ");
        out.push_identifier(self.1.as_str())
    }
}

impl QueryFragment for Op {
    fn to_sql(&self, out: &mut QueryBuilder) -> BuildQueryResult {
        // No escaping needed.
        out.push_sql(self.0.as_str());
        Ok(())
    }
}

impl QueryFragment for Constraint {
    fn to_sql(&self, out: &mut QueryBuilder) -> BuildQueryResult {
        use self::Constraint::*;
        match self {
            &Infix { ref op, ref left, ref right } => {
                left.to_sql(out)?;
                out.push_sql(" ");
                op.to_sql(out)?;
                out.push_sql(" ");
                right.to_sql(out)
            }
        }
    }
}

impl QueryFragment for JoinOp {
    fn to_sql(&self, out: &mut QueryBuilder) -> BuildQueryResult {
        out.push_sql(" JOIN ");
        Ok(())
    }
}

// We don't own SourceAlias or QueryFragment, so we can't implement the trait.
fn source_alias_to_sql(out: &mut QueryBuilder, sa: &SourceAlias) -> BuildQueryResult {
    let &SourceAlias(ref table, ref alias) = sa;
    out.push_identifier(table.name())?;
    out.push_sql(" AS ");
    out.push_identifier(alias.as_str())
}

impl QueryFragment for TableList {
    fn to_sql(&self, out: &mut QueryBuilder) -> BuildQueryResult {
        if self.0.is_empty() {
            return Ok(());
        }

        source_alias_to_sql(out, &self.0[0])?;

        for sa in self.0[1..].iter() {
            out.push_sql(", ");
            source_alias_to_sql(out, sa)?;
        }
        Ok(())
    }
}

impl QueryFragment for Join {
    fn to_sql(&self, out: &mut QueryBuilder) -> BuildQueryResult {
        self.left.to_sql(out)?;
        self.op.to_sql(out)?;
        self.right.to_sql(out)
    }
}

impl QueryFragment for TableOrSubquery {
    fn to_sql(&self, out: &mut QueryBuilder) -> BuildQueryResult {
        use self::TableOrSubquery::*;
        match self {
            &Table(ref sa) => source_alias_to_sql(out, sa)
        }
    }
}

impl QueryFragment for FromClause {
    fn to_sql(&self, out: &mut QueryBuilder) -> BuildQueryResult {
        use self::FromClause::*;
        match self {
            &TableList(ref table_list) => table_list.to_sql(out),
            &Join(ref join) => join.to_sql(out),
        }
    }
}

impl QueryFragment for SelectQuery {
    fn to_sql(&self, out: &mut QueryBuilder) -> BuildQueryResult {
        out.push_sql("SELECT ");
        self.projection[0].to_sql(out)?;

        for projection in self.projection[1..].iter() {
            out.push_sql(", ");
            projection.to_sql(out)?;
        }

        out.push_sql(" FROM ");
        self.from.to_sql(out)?;

        if self.constraints.is_empty() {
            return Ok(());
        }

        out.push_sql(" WHERE ");
        self.constraints[0].to_sql(out)?;

        for constraint in self.constraints[1..].iter() {
            out.push_sql(" AND ");
            constraint.to_sql(out)?;
        }

        Ok(())
    }
}

impl SelectQuery {
    fn to_sql_string(&self) -> Result<String, BuildQueryError> {
        let mut builder = SQLiteQueryBuilder::new();
        self.to_sql(&mut builder).map(|_| builder.finish())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_end_to_end() {

        // [:find ?x :where [?x 65537 ?v] [?x 65536 ?v]]
        let datoms00 = "datoms00".to_string();
        let datoms01 = "datoms01".to_string();
        let eq = Op("=".to_string());
        let source_aliases = vec![
            SourceAlias(DatomsTable::Datoms, datoms00.clone()),
            SourceAlias(DatomsTable::Datoms, datoms01.clone()),
        ];
        let query = SelectQuery {
            projection: vec![
                Projection(
                    ColumnOrExpression::Column(QualifiedAlias(datoms00.clone(), DatomsColumn::Entity)),
                    "x".to_string(),
                    ),
            ],
            from: FromClause::TableList(TableList(source_aliases)),
            constraints: vec![
                //ColumnOrExpression::Expression(TypedValue::Integer(15)),
                Constraint::Infix {
                    op: eq.clone(),
                    left: ColumnOrExpression::Column(QualifiedAlias(datoms01.clone(), DatomsColumn::Value)),
                    right: ColumnOrExpression::Column(QualifiedAlias(datoms00.clone(), DatomsColumn::Value)),
                },
                Constraint::Infix {
                    op: eq.clone(),
                    left: ColumnOrExpression::Column(QualifiedAlias(datoms00.clone(), DatomsColumn::Attribute)),
                    right: ColumnOrExpression::Integer(65537),
                },
                Constraint::Infix {
                    op: eq.clone(),
                    left: ColumnOrExpression::Column(QualifiedAlias(datoms01.clone(), DatomsColumn::Attribute)),
                    right: ColumnOrExpression::Integer(65536),
                },
            ],
        };

        let sql = query.to_sql_string().unwrap();
        println!("{}", sql);
    }
}
