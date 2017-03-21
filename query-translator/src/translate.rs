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

use mentat_core::{
    SQLValueType,
    TypedValue,
    ValueType,
};

use mentat_query::{
    Element,
    FindSpec,
    PlainSymbol,
    Variable,
};

use mentat_query_algebrizer::{
    AlgebraicQuery,
    ColumnConstraint,
    ConjoiningClauses,
    DatomsColumn,
    DatomsTable,
    QualifiedAlias,
    QueryValue,
    SourceAlias,
};

use mentat_query_projector::{
    CombinedProjection,
    Projector,
    query_projection,
};

use mentat_query_sql::{
    ColumnOrExpression,
    Constraint,
    FromClause,
    Name,
    Op,
    Projection,
    ProjectedColumn,
    SelectQuery,
    TableList,
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

            HasType(table, value_type) => {
                let column = QualifiedAlias(table, DatomsColumn::ValueTypeTag).to_column();
                Constraint::equal(column, ColumnOrExpression::Integer(value_type.value_type_tag()))
            },
        }
    }
}

pub struct ProjectedSelect{
    pub query: SelectQuery,
    pub projector: Box<Projector>,
}

/// Returns a `SelectQuery` that queries for the provided `cc`. Note that this _always_ returns a
/// query that runs SQL. The next level up the call stack can check for known-empty queries if
/// needed.
fn cc_to_select_query<T: Into<Option<u64>>>(projection: Projection, cc: ConjoiningClauses, limit: T) -> SelectQuery {
    let from = if cc.from.is_empty() {
        FromClause::Nothing
    } else {
        FromClause::TableList(TableList(cc.from))
    };
    SelectQuery {
        projection: projection,
        from: from,
        constraints: cc.wheres
                       .into_iter()
                       .map(|c| c.to_constraint())
                       .collect(),
        limit: if cc.is_known_empty { Some(0) } else { limit.into() },
    }
}

/// Return a query that projects `1` if the `cc` matches the store, and returns no results
/// if it doesn't.
pub fn cc_to_exists(cc: ConjoiningClauses) -> SelectQuery {
    if cc.is_known_empty {
        // In this case we can produce a very simple query that returns no results.
        SelectQuery {
            projection: Projection::One,
            from: FromClause::Nothing,
            constraints: vec![],
            limit: Some(0),
        }
    } else {
        cc_to_select_query(Projection::One, cc, 1)
    }
}

/// Consume a provided `ConjoiningClauses` to yield a new
/// `ProjectedSelect`. A projection list must also be provided.
fn cc_to_select(projection: CombinedProjection, cc: ConjoiningClauses, limit: Option<u64>) -> ProjectedSelect {
    let CombinedProjection { sql_projection, datalog_projector } = projection;
    ProjectedSelect {
        query: cc_to_select_query(sql_projection, cc, limit),
        projector: datalog_projector,
    }
}

/// Consume a provided `AlgebraicQuery` to yield a new
/// `ProjectedSelect`.
pub fn query_to_select(query: AlgebraicQuery) -> ProjectedSelect {
    cc_to_select(query_projection(&query), query.cc, query.limit)
}
