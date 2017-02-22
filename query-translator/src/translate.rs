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
    ColumnConstraint,
    ConjoiningClauses,
    DatomsColumn,
    DatomsTable,
    QualifiedAlias,
    SourceAlias,
};

use types::{
    ColumnOrExpression,
    Constraint,
    FromClause,
    Projection,
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
        }
    }
}


/// Consume a provided `ConjoiningClauses` to yield a new
/// `SelectQuery`. A projection list must also be provided.
pub fn cc_to_select(projection: Projection, cc: ConjoiningClauses) -> SelectQuery {
    SelectQuery {
        projection: projection,
        from: FromClause::TableList(TableList(cc.from)),
        constraints: cc.wheres
                       .into_iter()
                       .map(|c| c.to_constraint())
                       .collect(),
    }
}

pub fn cc_to_exists(cc: ConjoiningClauses) -> SelectQuery {
    cc_to_select(Projection::One, cc)
}
