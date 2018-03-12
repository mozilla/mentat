// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate regex;
extern crate mentat_core;
extern crate mentat_query;
extern crate mentat_query_algebrizer;
extern crate mentat_sql;

use std::boxed::Box;
use mentat_core::{
    Entid,
    SQLTypeAffinity,
    TypedValue,
    ValueType,
};

use mentat_query::{
    Direction,
    Limit,
    Variable,
};

use mentat_query_algebrizer::{
    Column,
    OrderBy,
    QualifiedAlias,
    QueryValue,
    SourceAlias,
    TableAlias,
    VariableColumn,
};

use mentat_sql::{
    BuildQueryResult,
    QueryBuilder,
    QueryFragment,
    SQLiteQueryBuilder,
    SQLQuery,
};

//---------------------------------------------------------
// A Mentat-focused representation of a SQL query.

/// One of the things that can appear in a projection or a constraint. Note that we use
/// `TypedValue` here; it's not pure SQL, but it avoids us having to concern ourselves at this
/// point with the translation between a `TypedValue` and the storage-layer representation.
///
/// Eventually we might allow different translations by providing a different `QueryBuilder`
/// implementation for each storage backend. Passing `TypedValue`s here allows for that.
pub enum ColumnOrExpression {
    Column(QualifiedAlias),
    ExistingColumn(Name),
    Entid(Entid),       // Because it's so common.
    Integer(i32),       // We use these for type codes etc.
    Long(i64),
    Value(TypedValue),
    Expression(Box<Expression>, ValueType),      // Track the return type.
}

pub enum Expression {
    Unary { sql_op: &'static str, arg: ColumnOrExpression },
}

/// `QueryValue` and `ColumnOrExpression` are almost identical… merge somehow?
impl From<QueryValue> for ColumnOrExpression {
    fn from(v: QueryValue) -> Self {
        match v {
            QueryValue::Column(c) => ColumnOrExpression::Column(c),
            QueryValue::Entid(e) => ColumnOrExpression::Entid(e),
            QueryValue::PrimitiveLong(v) => ColumnOrExpression::Long(v),
            QueryValue::TypedValue(v) => ColumnOrExpression::Value(v),
        }
    }
}

pub type Name = String;

pub struct ProjectedColumn(pub ColumnOrExpression, pub Name);

pub enum Projection {
    Columns(Vec<ProjectedColumn>),
    Star,
    One,
}

#[derive(Debug, PartialEq, Eq)]
pub enum GroupBy {
    ProjectedColumn(Name),
    QueryColumn(QualifiedAlias),
    // TODO: non-projected expressions, etc.
}

impl QueryFragment for GroupBy {
    fn push_sql(&self, out: &mut QueryBuilder) -> BuildQueryResult {
        match self {
            &GroupBy::ProjectedColumn(ref name) => {
                out.push_identifier(name.as_str())
            },
            &GroupBy::QueryColumn(ref qa) => {
                qualified_alias_push_sql(out, qa)
            },
        }
    }
}

#[derive(Copy, Clone)]
pub struct Op(pub &'static str);      // TODO: we can do better than this!

pub enum Constraint {
    Infix {
        op: Op,
        left: ColumnOrExpression,
        right: ColumnOrExpression,
    },
    Or {
        constraints: Vec<Constraint>,
    },
    And {
        constraints: Vec<Constraint>,
    },
    In {
        left: ColumnOrExpression,
        list: Vec<ColumnOrExpression>,
    },
    NotExists {
        subquery: TableOrSubquery,
    },
    TypeCheck {
        value: ColumnOrExpression,
        affinity: SQLTypeAffinity
    }
}

impl Constraint {
    pub fn not_equal(left: ColumnOrExpression, right: ColumnOrExpression) -> Constraint {
        Constraint::Infix {
            op: Op("<>"),     // ANSI SQL for future-proofing!
            left: left,
            right: right,
        }
    }

    pub fn equal(left: ColumnOrExpression, right: ColumnOrExpression) -> Constraint {
        Constraint::Infix {
            op: Op("="),
            left: left,
            right: right,
        }
    }

    pub fn fulltext_match(left: ColumnOrExpression, right: ColumnOrExpression) -> Constraint {
        Constraint::Infix {
            op: Op("MATCH"), // SQLite specific!
            left: left,
            right: right,
        }
    }
}

#[allow(dead_code)]
enum JoinOp {
    Inner,
}

// Short-hand for a list of tables all inner-joined.
pub struct TableList(pub Vec<TableOrSubquery>);

impl TableList {
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

pub struct Join {
    left: TableOrSubquery,
    op: JoinOp,
    right: TableOrSubquery,
    // TODO: constraints (ON, USING).
}

#[allow(dead_code)]
pub enum TableOrSubquery {
    Table(SourceAlias),
    Union(Vec<SelectQuery>, TableAlias),
    Subquery(Box<SelectQuery>),
    Values(Values, TableAlias),
}

pub enum Values {
    /// Like "VALUES (0, 1), (2, 3), ...".
    /// The vector must be of a length that is a multiple of the given size.
    Unnamed(usize, Vec<TypedValue>),

    /// Like "SELECT 0 AS x, SELECT 0 AS y WHERE 0 UNION ALL VALUES (0, 1), (2, 3), ...".
    /// The vector of values must be of a length that is a multiple of the length
    /// of the vector of names.
    Named(Vec<Variable>, Vec<TypedValue>),
}

pub enum FromClause {
    TableList(TableList),      // Short-hand for a pile of inner joins.
    Join(Join),
    Nothing,
}

pub struct SelectQuery {
    pub distinct: bool,
    pub projection: Projection,
    pub from: FromClause,
    pub constraints: Vec<Constraint>,
    pub group_by: Vec<GroupBy>,
    pub order: Vec<OrderBy>,
    pub limit: Limit,
}

fn push_variable_column(qb: &mut QueryBuilder, vc: &VariableColumn) -> BuildQueryResult {
    match vc {
        &VariableColumn::Variable(ref v) => {
            qb.push_identifier(v.as_str())
        },
        &VariableColumn::VariableTypeTag(ref v) => {
            qb.push_identifier(format!("{}_value_type_tag", v.name()).as_str())
        },
    }
}

fn push_column(qb: &mut QueryBuilder, col: &Column) -> BuildQueryResult {
    match col {
        &Column::Fixed(ref d) => {
            qb.push_sql(d.as_str());
            Ok(())
        },
        &Column::Fulltext(ref d) => {
            qb.push_sql(d.as_str());
            Ok(())
        },
        &Column::Variable(ref vc) => push_variable_column(qb, vc),
    }
}

//---------------------------------------------------------
// Turn that representation into SQL.


/// A helper macro to sequentially process an iterable sequence,
/// evaluating a block between each pair of items.
///
/// This is used to simply and efficiently produce output like
///
/// ```sql
///   1, 2, 3
/// ```
///
/// or
///
/// ```sql
/// x = 1 AND y = 2
/// ```
///
/// without producing an intermediate string sequence.
macro_rules! interpose {
    ( $name: pat, $across: expr, $body: block, $inter: block ) => {
        interpose_iter!($name, $across.iter(), $body, $inter)
    }
}

macro_rules! interpose_iter {
    ( $name: pat, $across: expr, $body: block, $inter: block ) => {
        let mut seq = $across;
        if let Some($name) = seq.next() {
            $body;
            for $name in seq {
                $inter;
                $body;
            }
        }
    }
}

impl QueryFragment for ColumnOrExpression {
    fn push_sql(&self, out: &mut QueryBuilder) -> BuildQueryResult {
        use self::ColumnOrExpression::*;
        match self {
            &Column(ref qa) => {
                qualified_alias_push_sql(out, qa)
            },
            &ExistingColumn(ref alias) => {
                out.push_identifier(alias.as_str())
            },
            &Entid(entid) => {
                out.push_sql(entid.to_string().as_str());
                Ok(())
            },
            &Integer(integer) => {
                out.push_sql(integer.to_string().as_str());
                Ok(())
            },
            &Long(long) => {
                out.push_sql(long.to_string().as_str());
                Ok(())
            },
            &Value(ref v) => {
                out.push_typed_value(v)
            },
            &Expression(ref e, _) => {
                e.push_sql(out)
            },
        }
    }
}

impl QueryFragment for Expression {
    fn push_sql(&self, out: &mut QueryBuilder) -> BuildQueryResult {
        match self {
            &Expression::Unary { ref sql_op, ref arg } => {
                out.push_sql(sql_op);              // No need to escape built-ins.
                out.push_sql("(");
                arg.push_sql(out)?;
                out.push_sql(")");
                Ok(())
            },
        }
    }
}

impl QueryFragment for Projection {
    fn push_sql(&self, out: &mut QueryBuilder) -> BuildQueryResult {
        use self::Projection::*;
        match self {
            &One => out.push_sql("1"),
            &Star => out.push_sql("*"),
            &Columns(ref cols) => {
                let &ProjectedColumn(ref col, ref alias) = &cols[0];
                col.push_sql(out)?;
                out.push_sql(" AS ");
                out.push_identifier(alias.as_str())?;

                for &ProjectedColumn(ref col, ref alias) in &cols[1..] {
                    out.push_sql(", ");
                    col.push_sql(out)?;
                    out.push_sql(" AS ");
                    out.push_identifier(alias.as_str())?;
                }
            },
        };
        Ok(())
    }
}

impl QueryFragment for Op {
    fn push_sql(&self, out: &mut QueryBuilder) -> BuildQueryResult {
        // No escaping needed.
        out.push_sql(self.0);
        Ok(())
    }
}

impl QueryFragment for Constraint {
    fn push_sql(&self, out: &mut QueryBuilder) -> BuildQueryResult {
        use self::Constraint::*;
        match self {
            &Infix { ref op, ref left, ref right } => {
                left.push_sql(out)?;
                out.push_sql(" ");
                op.push_sql(out)?;
                out.push_sql(" ");
                right.push_sql(out)
            },

            &And { ref constraints } => {
                // An empty intersection is true.
                if constraints.is_empty() {
                    out.push_sql("1");
                    return Ok(())
                }
                out.push_sql("(");
                interpose!(constraint, constraints,
                           { constraint.push_sql(out)? },
                           { out.push_sql(" AND ") });
                out.push_sql(")");
                Ok(())
            },

            &Or { ref constraints } => {
                // An empty alternation is false.
                if constraints.is_empty() {
                    out.push_sql("0");
                    return Ok(())
                }
                out.push_sql("(");
                interpose!(constraint, constraints,
                           { constraint.push_sql(out)? },
                           { out.push_sql(" OR ") });
                out.push_sql(")");
                Ok(())
            }

            &In { ref left, ref list } => {
                left.push_sql(out)?;
                out.push_sql(" IN (");
                interpose!(item, list,
                           { item.push_sql(out)? },
                           { out.push_sql(", ") });
                out.push_sql(")");
                Ok(())
            },
            &NotExists { ref subquery } => {
                out.push_sql("NOT EXISTS ");
                subquery.push_sql(out)
            },
            &TypeCheck { ref value, ref affinity } => {
                out.push_sql("typeof(");
                value.push_sql(out)?;
                out.push_sql(") = ");
                out.push_sql(match *affinity {
                    SQLTypeAffinity::Null => "'null'",
                    SQLTypeAffinity::Integer => "'integer'",
                    SQLTypeAffinity::Real => "'real'",
                    SQLTypeAffinity::Text => "'text'",
                    SQLTypeAffinity::Blob => "'blob'",
                });
                Ok(())
            },
        }
    }
}

impl QueryFragment for JoinOp {
    fn push_sql(&self, out: &mut QueryBuilder) -> BuildQueryResult {
        out.push_sql(" JOIN ");
        Ok(())
    }
}

// We don't own QualifiedAlias or QueryFragment, so we can't implement the trait.
fn qualified_alias_push_sql(out: &mut QueryBuilder, qa: &QualifiedAlias) -> BuildQueryResult {
    out.push_identifier(qa.0.as_str())?;
    out.push_sql(".");
    push_column(out, &qa.1)
}

// We don't own SourceAlias or QueryFragment, so we can't implement the trait.
fn source_alias_push_sql(out: &mut QueryBuilder, sa: &SourceAlias) -> BuildQueryResult {
    let &SourceAlias(ref table, ref alias) = sa;
    out.push_identifier(table.name())?;
    out.push_sql(" AS ");
    out.push_identifier(alias.as_str())
}

impl QueryFragment for TableList {
    fn push_sql(&self, out: &mut QueryBuilder) -> BuildQueryResult {
        if self.0.is_empty() {
            return Ok(());
        }

        interpose!(t, self.0,
                   { t.push_sql(out)? },
                   { out.push_sql(", ") });
        Ok(())
    }
}

impl QueryFragment for Join {
    fn push_sql(&self, out: &mut QueryBuilder) -> BuildQueryResult {
        self.left.push_sql(out)?;
        self.op.push_sql(out)?;
        self.right.push_sql(out)
    }
}

impl QueryFragment for TableOrSubquery {
    fn push_sql(&self, out: &mut QueryBuilder) -> BuildQueryResult {
        use self::TableOrSubquery::*;
        match self {
            &Table(ref sa) => source_alias_push_sql(out, sa),
            &Union(ref subqueries, ref table_alias) => {
                out.push_sql("(");
                interpose!(subquery, subqueries,
                           { subquery.push_sql(out)? },
                           { out.push_sql(" UNION ") });
                out.push_sql(") AS ");
                out.push_identifier(table_alias.as_str())
            },
            &Subquery(ref subquery) => {
                out.push_sql("(");
                subquery.push_sql(out)?;
                out.push_sql(")");
                Ok(())
            },
            &Values(ref values, ref table_alias) => {
                // XXX: does this work for Values::Unnamed?
                out.push_sql("(");
                values.push_sql(out)?;
                out.push_sql(") AS ");
                out.push_identifier(table_alias.as_str())
            },
        }
    }
}

impl QueryFragment for Values {
    fn push_sql(&self, out: &mut QueryBuilder) -> BuildQueryResult {
        // There are at least 3 ways to name the columns of a VALUES table:
        // 1) the columns are named "", ":1", ":2", ... -- but this is undocumented.  See
        //    http://stackoverflow.com/a/40921724.
        // 2) A CTE ("WITH" statement) can declare the shape of the table, like "WITH
        //    table_name(column_name, ...) AS (VALUES ...)".
        // 3) We can "UNION ALL" a dummy "SELECT" statement in place.
        //
        // We don't want to use an undocumented SQLite quirk, and we're a little concerned that some
        // SQL systems will not optimize WITH statements well.  It's also convenient to have an in
        // place table to query, so for now we implement option 3.
        if let &Values::Named(ref names, _) = self {
            out.push_sql("SELECT ");
            interpose!(alias, names,
                       { out.push_sql("0 AS ");
                         out.push_identifier(alias.as_str())? },
                       { out.push_sql(", ") });

            out.push_sql(" WHERE 0 UNION ALL ");
        }

        let values = match self {
            &Values::Named(ref names, ref values) => values.chunks(names.len()),
            &Values::Unnamed(ref size, ref values) => values.chunks(*size),
        };

        out.push_sql("VALUES ");

        interpose_iter!(outer, values,
                        { out.push_sql("(");
                          interpose!(inner, outer,
                                     { out.push_typed_value(inner)? },
                                     { out.push_sql(", ") });
                          out.push_sql(")");
                        },
                        { out.push_sql(", ") });
        Ok(())
    }
}

impl QueryFragment for FromClause {
    fn push_sql(&self, out: &mut QueryBuilder) -> BuildQueryResult {
        use self::FromClause::*;
        match self {
            &TableList(ref table_list) => {
                if table_list.is_empty() {
                    Ok(())
                } else {
                    out.push_sql(" FROM ");
                    table_list.push_sql(out)
                }
            },
            &Join(ref join) => {
                out.push_sql(" FROM ");
                join.push_sql(out)
            },
            &Nothing => Ok(()),
        }
    }
}

impl SelectQuery {
    fn push_variable_param(&self, var: &Variable, out: &mut QueryBuilder) -> BuildQueryResult {
        // `var` is something like `?foo99-people`.
        // Trim the `?` and escape the rest. Prepend `i` to distinguish from
        // the inline value space `v`.
        let re = regex::Regex::new("[^a-zA-Z_0-9]").unwrap();
        let without_question = var.as_str().split_at(1).1;
        let replaced = re.replace_all(without_question, "_");
        let bind_param = format!("i{}", replaced);                 // We _could_ avoid this copying.
        out.push_bind_param(bind_param.as_str())
    }
}

impl QueryFragment for SelectQuery {
    fn push_sql(&self, out: &mut QueryBuilder) -> BuildQueryResult {
        if self.distinct {
            out.push_sql("SELECT DISTINCT ");
        } else {
            out.push_sql("SELECT ");
        }
        self.projection.push_sql(out)?;
        self.from.push_sql(out)?;

        if !self.constraints.is_empty() {
            out.push_sql(" WHERE ");
            interpose!(constraint, self.constraints,
                       { constraint.push_sql(out)? },
                       { out.push_sql(" AND ") });
        }

        match &self.group_by {
            group_by if !group_by.is_empty() => {
                out.push_sql(" GROUP BY ");
                interpose!(group, group_by,
                           { group.push_sql(out)? },
                           { out.push_sql(", ") });
            },
            _ => {},
        }

        if !self.order.is_empty() {
            out.push_sql(" ORDER BY ");
            interpose!(&OrderBy(ref dir, ref var), self.order,
                       { push_variable_column(out, var)?;
                         match dir {
                             &Direction::Ascending => { out.push_sql(" ASC"); },
                             &Direction::Descending => { out.push_sql(" DESC"); },
                         };
                       },
                       { out.push_sql(", ") });
        }

        match &self.limit {
            &Limit::None => (),
            &Limit::Fixed(limit) => {
                // Guaranteed to be non-negative: u64.
                out.push_sql(" LIMIT ");
                out.push_sql(limit.to_string().as_str());
            },
            &Limit::Variable(ref var) => {
                // Guess this wasn't bound yet. Produce an argument.
                out.push_sql(" LIMIT ");
                self.push_variable_param(var, out)?;
            },
        }

        Ok(())
    }
}

impl SelectQuery {
    pub fn to_sql_query(&self) -> mentat_sql::Result<SQLQuery> {
        let mut builder = SQLiteQueryBuilder::new();
        self.push_sql(&mut builder).map(|_| builder.finish())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::rc::Rc;

    use mentat_query_algebrizer::{
        Column,
        DatomsColumn,
        DatomsTable,
        FulltextColumn,
    };

    fn build_query(c: &QueryFragment) -> SQLQuery {
        let mut builder = SQLiteQueryBuilder::new();
        c.push_sql(&mut builder)
         .map(|_| builder.finish())
         .expect("to produce a query for the given constraint")
    }

    fn build(c: &QueryFragment) -> String {
        build_query(c).sql
    }

    #[test]
    fn test_in_constraint() {
        let none = Constraint::In {
            left: ColumnOrExpression::Column(QualifiedAlias::new("datoms01".to_string(), Column::Fixed(DatomsColumn::Value))),
            list: vec![],
        };

        let one = Constraint::In {
            left: ColumnOrExpression::Column(QualifiedAlias::new("datoms01".to_string(), DatomsColumn::Value)),
            list: vec![
                ColumnOrExpression::Entid(123),
            ],
        };

        let three = Constraint::In {
            left: ColumnOrExpression::Column(QualifiedAlias::new("datoms01".to_string(), DatomsColumn::Value)),
            list: vec![
                ColumnOrExpression::Entid(123),
                ColumnOrExpression::Entid(456),
                ColumnOrExpression::Entid(789),
            ],
        };

        assert_eq!("`datoms01`.v IN ()", build(&none));
        assert_eq!("`datoms01`.v IN (123)", build(&one));
        assert_eq!("`datoms01`.v IN (123, 456, 789)", build(&three));
    }

    #[test]
    fn test_and_constraint() {
        let c = Constraint::And {
            constraints: vec![
                Constraint::And {
                    constraints: vec![
                        Constraint::Infix {
                            op: Op("="),
                            left: ColumnOrExpression::Entid(123),
                            right: ColumnOrExpression::Entid(456),
                        },
                        Constraint::Infix {
                            op: Op("="),
                            left: ColumnOrExpression::Entid(789),
                            right: ColumnOrExpression::Entid(246),
                        },
                    ],
                },
            ],
        };

        // Two sets of parens: the outermost AND only has one child,
        // but still contributes parens.
        assert_eq!("((123 = 456 AND 789 = 246))", build(&c));
    }

    #[test]
    fn test_unnamed_values() {
        let build = |len, values| build(&Values::Unnamed(len, values));

        assert_eq!(build(1, vec![TypedValue::Long(1)]),
                   "VALUES (1)");

        assert_eq!(build(2, vec![TypedValue::Boolean(false), TypedValue::Long(1)]),
                   "VALUES (0, 1)");

        assert_eq!(build(2, vec![TypedValue::Boolean(false), TypedValue::Long(1),
                                 TypedValue::Boolean(true), TypedValue::Long(2)]),
                   "VALUES (0, 1), (1, 2)");
    }

    #[test]
    fn test_named_values() {
        let build = |names: Vec<_>, values| build(&Values::Named(names.into_iter().map(Variable::from_valid_name).collect(), values));
        assert_eq!(build(vec!["?a"], vec![TypedValue::Long(1)]),
                   "SELECT 0 AS `?a` WHERE 0 UNION ALL VALUES (1)");

        assert_eq!(build(vec!["?a", "?b"], vec![TypedValue::Boolean(false), TypedValue::Long(1)]),
                   "SELECT 0 AS `?a`, 0 AS `?b` WHERE 0 UNION ALL VALUES (0, 1)");

        assert_eq!(build(vec!["?a", "?b"],
                         vec![TypedValue::Boolean(false), TypedValue::Long(1),
                              TypedValue::Boolean(true), TypedValue::Long(2)]),
                   "SELECT 0 AS `?a`, 0 AS `?b` WHERE 0 UNION ALL VALUES (0, 1), (1, 2)");
    }

    #[test]
    fn test_matches_constraint() {
        let c = Constraint::Infix {
            op: Op("MATCHES"),
            left: ColumnOrExpression::Column(QualifiedAlias("fulltext01".to_string(), Column::Fulltext(FulltextColumn::Text))),
            right: ColumnOrExpression::Value(TypedValue::String(Rc::new("needle".to_string()))),
        };
        let q = build_query(&c);
        assert_eq!("`fulltext01`.text MATCHES $v0", q.sql);
        assert_eq!(vec![("$v0".to_string(), Rc::new(mentat_sql::Value::Text("needle".to_string())))], q.args);

        let c = Constraint::Infix {
            op: Op("="),
            left: ColumnOrExpression::Column(QualifiedAlias("fulltext01".to_string(), Column::Fulltext(FulltextColumn::Rowid))),
            right: ColumnOrExpression::Column(QualifiedAlias("datoms02".to_string(), Column::Fixed(DatomsColumn::Value))),
        };
        assert_eq!("`fulltext01`.rowid = `datoms02`.v", build(&c));
    }

    #[test]
    fn test_end_to_end() {
        // [:find ?x :where [?x 65537 ?v] [?x 65536 ?v]]
        let datoms00 = "datoms00".to_string();
        let datoms01 = "datoms01".to_string();
        let eq = Op("=");
        let source_aliases = vec![
            TableOrSubquery::Table(SourceAlias(DatomsTable::Datoms, datoms00.clone())),
            TableOrSubquery::Table(SourceAlias(DatomsTable::Datoms, datoms01.clone())),
        ];

        let mut query = SelectQuery {
            distinct: true,
            projection: Projection::Columns(
                            vec![
                                ProjectedColumn(
                                    ColumnOrExpression::Column(QualifiedAlias::new(datoms00.clone(), DatomsColumn::Entity)),
                                    "x".to_string()),
                            ]),
            from: FromClause::TableList(TableList(source_aliases)),
            constraints: vec![
                Constraint::Infix {
                    op: eq.clone(),
                    left: ColumnOrExpression::Column(QualifiedAlias::new(datoms01.clone(), DatomsColumn::Value)),
                    right: ColumnOrExpression::Column(QualifiedAlias::new(datoms00.clone(), DatomsColumn::Value)),
                },
                Constraint::Infix {
                    op: eq.clone(),
                    left: ColumnOrExpression::Column(QualifiedAlias::new(datoms00.clone(), DatomsColumn::Attribute)),
                    right: ColumnOrExpression::Entid(65537),
                },
                Constraint::Infix {
                    op: eq.clone(),
                    left: ColumnOrExpression::Column(QualifiedAlias::new(datoms01.clone(), DatomsColumn::Attribute)),
                    right: ColumnOrExpression::Entid(65536),
                },
            ],
            group_by: vec![],
            order: vec![],
            limit: Limit::None,
        };

        let SQLQuery { sql, args } = query.to_sql_query().unwrap();
        println!("{}", sql);
        assert_eq!("SELECT DISTINCT `datoms00`.e AS `x` FROM `datoms` AS `datoms00`, `datoms` AS `datoms01` WHERE `datoms01`.v = `datoms00`.v AND `datoms00`.a = 65537 AND `datoms01`.a = 65536", sql);
        assert!(args.is_empty());

        // And without distinct…
        query.distinct = false;
        let SQLQuery { sql, args } = query.to_sql_query().unwrap();
        println!("{}", sql);
        assert_eq!("SELECT `datoms00`.e AS `x` FROM `datoms` AS `datoms00`, `datoms` AS `datoms01` WHERE `datoms01`.v = `datoms00`.v AND `datoms00`.a = 65537 AND `datoms01`.a = 65536", sql);
        assert!(args.is_empty());

    }
}
