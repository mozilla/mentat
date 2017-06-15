// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use mentat_core::{
    SQLValueType,
    TypedValue,
    ValueType,
};

use mentat_query::Limit;

use mentat_query_algebrizer::{
    AlgebraicQuery,
    ColumnAlternation,
    ColumnConstraint,
    ColumnConstraintOrAlternation,
    ColumnIntersection,
    ColumnName,
    ComputedTable,
    ConjoiningClauses,
    DatomsColumn,
    DatomsTable,
    OrderBy,
    QualifiedAlias,
    QueryValue,
    SourceAlias,
    TableAlias,
    VariableColumn,
};

use mentat_query_projector::{
    CombinedProjection,
    Projector,
    projected_column_for_var,
    query_projection,
};

use mentat_query_sql::{
    ColumnOrExpression,
    Constraint,
    FromClause,
    Op,
    ProjectedColumn,
    Projection,
    SelectQuery,
    TableList,
    TableOrSubquery,
    Values,
};

trait ToConstraint {
    fn to_constraint(self) -> Constraint;
}

trait ToColumn {
    fn to_column(self) -> ColumnOrExpression;
}

impl ToColumn for QualifiedAlias {
    fn to_column(self) -> ColumnOrExpression {
        ColumnOrExpression::Column(self)
    }
}

impl ToConstraint for ColumnIntersection {
    fn to_constraint(self) -> Constraint {
        Constraint::And {
            constraints: self.into_iter().map(|x| x.to_constraint()).collect()
        }
    }
}

impl ToConstraint for ColumnAlternation {
    fn to_constraint(self) -> Constraint {
        Constraint::Or {
            constraints: self.into_iter().map(|x| x.to_constraint()).collect()
        }
    }
}

impl ToConstraint for ColumnConstraintOrAlternation {
    fn to_constraint(self) -> Constraint {
        use self::ColumnConstraintOrAlternation::*;
        match self {
            Alternation(alt) => alt.to_constraint(),
            Constraint(c) => c.to_constraint(),
        }
    }
}

impl ToConstraint for ColumnConstraint {
    fn to_constraint(self) -> Constraint {
        use self::ColumnConstraint::*;
        match self {
            Equals(qa, QueryValue::Entid(entid)) =>
                Constraint::equal(qa.to_column(), ColumnOrExpression::Entid(entid)),

            Equals(qa, QueryValue::TypedValue(tv)) =>
                Constraint::equal(qa.to_column(), ColumnOrExpression::Value(tv)),

            Equals(left, QueryValue::Column(right)) =>
                Constraint::equal(left.to_column(), right.to_column()),

            Equals(qa, QueryValue::PrimitiveLong(value)) => {
                let tag_column = qa.for_type_tag().to_column();
                let value_column = qa.to_column();

                /// A bare long in a query might match a ref, an instant, a long (obviously), or a
                /// double. If it's negative, it can't match a ref, but that's OK -- it won't!
                ///
                /// However, '1' and '0' are used to represent booleans, and some integers are also
                /// used to represent FTS values. We don't want to accidentally match those.
                ///
                /// We ask `SQLValueType` whether this value is in range for how booleans are
                /// represented in the database.
                ///
                /// We only hit this code path when the attribute is unknown, so we're querying
                /// `all_datoms`. That means we don't see FTS IDs at all -- they're transparently
                /// replaced by their strings. If that changes, then you should also exclude the
                /// string type code (10) here.
                let must_exclude_boolean = ValueType::Boolean.accommodates_integer(value);
                if must_exclude_boolean {
                    Constraint::And {
                        constraints: vec![
                            Constraint::equal(value_column,
                                              ColumnOrExpression::Value(TypedValue::Long(value))),
                            Constraint::not_equal(tag_column,
                                                  ColumnOrExpression::Integer(ValueType::Boolean.value_type_tag())),
                        ],
                    }
                } else {
                    Constraint::equal(value_column, ColumnOrExpression::Value(TypedValue::Long(value)))
                }
            },

            NumericInequality { operator, left, right } => {
                Constraint::Infix {
                    op: Op(operator.to_sql_operator()),
                    left: left.into(),
                    right: right.into(),
                }
            },

            Matches(left, right) => {
                Constraint::Infix {
                    op: Op("MATCH"),
                    left: ColumnOrExpression::Column(left),
                    right: right.into(),
                }
            },

            HasType(table, value_type) => {
                let column = QualifiedAlias::new(table, DatomsColumn::ValueTypeTag).to_column();
                Constraint::equal(column, ColumnOrExpression::Integer(value_type.value_type_tag()))
            },

            NotExists(computed_table) => {
                let subquery = table_for_computed(computed_table, TableAlias::new());
                Constraint::NotExists {
                    subquery: subquery,
                }
            },
        }
    }
}

pub struct ProjectedSelect{
    pub query: SelectQuery,
    pub projector: Box<Projector>,
}

// Nasty little hack to let us move out of indexed context.
struct ConsumableVec<T> {
    inner: Vec<Option<T>>,
}

impl<T> From<Vec<T>> for ConsumableVec<T> {
    fn from(vec: Vec<T>) -> ConsumableVec<T> {
        ConsumableVec { inner: vec.into_iter().map(|x| Some(x)).collect() }
    }
}

impl<T> ConsumableVec<T> {
    fn take_dangerously(&mut self, i: usize) -> T {
        ::std::mem::replace(&mut self.inner[i], None).expect("each value to only be fetched once")
    }
}

fn table_for_computed(computed: ComputedTable, alias: TableAlias) -> TableOrSubquery {
    match computed {
        ComputedTable::Union {
            projection, type_extraction, arms,
        } => {
            // The projection list for each CC must have the same shape and the same names.
            // The values we project might be fixed or they might be columns.
            TableOrSubquery::Union(
                arms.into_iter()
                    .map(|cc| {
                        // We're going to end up with the variables being projected and also some
                        // type tag columns.
                        let mut columns: Vec<ProjectedColumn> = Vec::with_capacity(projection.len() + type_extraction.len());

                        // For each variable, find out which column it maps to within this arm, and
                        // project it as the variable name.
                        // E.g., SELECT datoms03.v AS `?x`.
                        for var in projection.iter() {
                            let (projected_column, maybe_type) = projected_column_for_var(var, &cc);
                            columns.push(projected_column);

                            // Similarly, project type tags if they're not known conclusively in the
                            // outer query.
                            // Assumption: we'll never need to project a tag without projecting the value of a variable.
                            if type_extraction.contains(var) {
                                let expression =
                                    if let Some(ty) = maybe_type {
                                        // If we know the type for sure, just project the constant.
                                        // SELECT datoms03.v AS `?x`, 10 AS `?x_value_type_tag`
                                        ColumnOrExpression::Integer(ty.value_type_tag())
                                    } else {
                                        // Otherwise, we'll have an established type binding! This'll be
                                        // either a datoms table or, recursively, a subquery. Project
                                        // this:
                                        // SELECT datoms03.v AS `?x`,
                                        //        datoms03.value_type_tag AS `?x_value_type_tag`
                                        let extract = cc.extracted_types
                                                        .get(var)
                                                        .expect("Expected variable to have a known type or an extracted type");
                                        ColumnOrExpression::Column(extract.clone())
                                    };
                                let type_column = VariableColumn::VariableTypeTag(var.clone());
                                let proj = ProjectedColumn(expression, type_column.column_name());
                                columns.push(proj);
                            }
                        }

                        // Each arm simply turns into a subquery.
                        // The SQL translation will stuff "UNION" between each arm.
                        let projection = Projection::Columns(columns);
                        cc_to_select_query(projection, cc, false, None, Limit::None)
                  }).collect(),
                alias)
        },
        ComputedTable::Subquery(subquery) => {
            TableOrSubquery::Subquery(Box::new(cc_to_exists(subquery)))
        },
        ComputedTable::NamedValues {
            names, values,
        } => {
            // We assume column homogeneity, so we won't have any type tag columns.
            TableOrSubquery::Values(Values::Named(names, values), alias)
        },
    }
}

/// Returns a `SelectQuery` that queries for the provided `cc`. Note that this _always_ returns a
/// query that runs SQL. The next level up the call stack can check for known-empty queries if
/// needed.
fn cc_to_select_query(projection: Projection,
                      cc: ConjoiningClauses,
                      distinct: bool,
                      order: Option<Vec<OrderBy>>,
                      limit: Limit) -> SelectQuery {
    let from = if cc.from.is_empty() {
        FromClause::Nothing
    } else {
        // Move these out of the CC.
        let from = cc.from;
        let mut computed: ConsumableVec<_> = cc.computed_tables.into();

        // Why do we put computed tables directly into the `FROM` clause? The alternative is to use
        // a CTE (`WITH`). They're typically equivalent, but some SQL systems (notably Postgres)
        // treat CTEs as optimization barriers, so a `WITH` can be significantly slower. Given that
        // this is easy enough to change later, we'll opt for using direct inclusion in `FROM`.
        let tables =
            from.into_iter().map(|source_alias| {
                match source_alias {
                    SourceAlias(DatomsTable::Computed(i), alias) => {
                        let comp = computed.take_dangerously(i);
                        table_for_computed(comp, alias)
                    },
                    _ => {
                        TableOrSubquery::Table(source_alias)
                    }
                }
            });

        FromClause::TableList(TableList(tables.collect()))
    };

    let order = order.map_or(vec![], |vec| { vec.into_iter().map(|o| o.into()).collect() });
    let limit = if cc.empty_because.is_some() { Limit::Fixed(0) } else { limit };
    SelectQuery {
        distinct: distinct,
        projection: projection,
        from: from,
        constraints: cc.wheres
                       .into_iter()
                       .map(|c| c.to_constraint())
                       .collect(),
        order: order,
        limit: limit,
    }
}

/// Return a query that projects `1` if the `cc` matches the store, and returns no results
/// if it doesn't.
pub fn cc_to_exists(cc: ConjoiningClauses) -> SelectQuery {
    if cc.is_known_empty() {
        // In this case we can produce a very simple query that returns no results.
        SelectQuery {
            distinct: false,
            projection: Projection::One,
            from: FromClause::Nothing,
            constraints: vec![],
            order: vec![],
            limit: Limit::None,
        }
    } else {
        cc_to_select_query(Projection::One, cc, false, None, Limit::None)
    }
}

/// Consume a provided `AlgebraicQuery` to yield a new
/// `ProjectedSelect`.
pub fn query_to_select(query: AlgebraicQuery) -> ProjectedSelect {
    // TODO: we can't pass `query.limit` here if we aggregate during projection.
    // SQL-based aggregation -- `SELECT SUM(datoms00.e)` -- is fine.
    let CombinedProjection { sql_projection, datalog_projector, distinct } = query_projection(&query);
    ProjectedSelect {
        query: cc_to_select_query(sql_projection, query.cc, distinct, query.order, query.limit),
        projector: datalog_projector,
    }
}
