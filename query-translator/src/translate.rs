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
            EqualsEntity(qa, entid) =>
                Constraint::equal(qa.to_column(), ColumnOrExpression::Entid(entid)),

            EqualsValue(qa, tv) =>
                Constraint::equal(qa.to_column(), ColumnOrExpression::Value(tv)),

            EqualsColumn(left, right) =>
                Constraint::equal(left.to_column(), right.to_column()),

            EqualsPrimitiveLong(table, value) => {
                let value_column = QualifiedAlias(table.clone(), DatomsColumn::Value).to_column();
                let tag_column = QualifiedAlias(table, DatomsColumn::ValueTypeTag).to_column();

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

            HasType(table, value_type) => {
                let column = QualifiedAlias(table, DatomsColumn::ValueTypeTag).to_column();
                Constraint::equal(column, ColumnOrExpression::Integer(value_type.value_type_tag()))
            },
        }
    }
}

pub struct CombinedSelectQuery {
    pub query: SelectQuery,
    pub projector: Box<Projector>,
}

fn cc_to_select_query<T: Into<Option<u64>>>(projection: Projection, cc: ConjoiningClauses, limit: T) -> SelectQuery {
    if cc.is_known_empty {
        SelectQuery {
            projection: Projection::One,
            from: FromClause::Nothing,
            constraints: vec![],
            limit: Some(0),
        }
    } else {
        SelectQuery {
            projection: projection,
            from: FromClause::TableList(TableList(cc.from)),
            constraints: cc.wheres
                           .into_iter()
                           .map(|c| c.to_constraint())
                           .collect(),
            limit: limit.into(),
        }
    }
}

/// Consume a provided `ConjoiningClauses` to yield a new
/// `SelectQuery`. A projection list must also be provided.
pub fn cc_to_select(projection: CombinedProjection, cc: ConjoiningClauses, limit: Option<u64>) -> CombinedSelectQuery {
    let CombinedProjection { sql_projection, datalog_projector } = projection;
    CombinedSelectQuery {
        query: cc_to_select_query(sql_projection, cc, limit),
        projector: datalog_projector,
    }
}

pub fn query_to_select(query: AlgebraicQuery) -> CombinedSelectQuery {
    cc_to_select(query_projection(&query), query.cc, query.limit)
}

pub fn cc_to_exists(cc: ConjoiningClauses) -> SelectQuery {
    cc_to_select_query(Projection::One, cc, 1)
}
