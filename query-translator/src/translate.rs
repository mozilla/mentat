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
                Constraint::And {
                    constraints: vec![
                        Constraint::equal(value_column, ColumnOrExpression::Value(TypedValue::Long(value))),
                        Constraint::In {
                            left: tag_column,
                            list:
                                if value < 0 {
                                    // Can't be a ref or a boolean.
                                    vec![ColumnOrExpression::Integer(4),
                                         ColumnOrExpression::Integer(5),]
                                } else if value > 1 {
                                    // Can't be a boolean.
                                    vec![ColumnOrExpression::Integer(0),
                                         ColumnOrExpression::Integer(4),
                                         ColumnOrExpression::Integer(5),]
                                } else {
                                    vec![ColumnOrExpression::Integer(0),
                                         ColumnOrExpression::Integer(1),
                                         ColumnOrExpression::Integer(4),
                                         ColumnOrExpression::Integer(5),]
                                },
                        },
                    ],
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
