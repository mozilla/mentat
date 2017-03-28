// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

// WIP
#![allow(dead_code, unused_imports)]

use mentat_core::{
    Entid,
    Schema,
    TypedValue,
    ValueType,
};

use mentat_query::{
    NonIntegerConstant,
    Pattern,
    PatternValuePlace,
    PatternNonValuePlace,
    PlainSymbol,
    Predicate,
    SrcVar,
};

use clauses::ConjoiningClauses;

use errors::{
    Result,
    Error,
    ErrorKind,
};

use types::{
    ColumnConstraint,
    ColumnIntersection,
    DatomsColumn,
    DatomsTable,
    EmptyBecause,
    NumericComparison,
    OrJoinKind,
    QualifiedAlias,
    QueryValue,
    SourceAlias,
    TableAlias,
};




/// Return true if both left and right are the same variable or both are non-variable.
fn _simply_matches_place(left: &PatternNonValuePlace, right: &PatternNonValuePlace) -> bool {
    match (left, right) {
        (&PatternNonValuePlace::Variable(ref a), &PatternNonValuePlace::Variable(ref b)) => a == b,
        (&PatternNonValuePlace::Placeholder, &PatternNonValuePlace::Placeholder) => true,
        (&PatternNonValuePlace::Entid(_), &PatternNonValuePlace::Entid(_))       => true,
        (&PatternNonValuePlace::Entid(_), &PatternNonValuePlace::Ident(_))       => true,
        (&PatternNonValuePlace::Ident(_), &PatternNonValuePlace::Ident(_))       => true,
        (&PatternNonValuePlace::Ident(_), &PatternNonValuePlace::Entid(_))       => true,
        _ => false,
    }
}

/// Return true if both left and right are the same variable or both are non-variable.
fn _simply_matches_value_place(left: &PatternValuePlace, right: &PatternValuePlace) -> bool {
    match (left, right) {
        (&PatternValuePlace::Variable(ref a), &PatternValuePlace::Variable(ref b)) => a == b,
        (&PatternValuePlace::Placeholder, &PatternValuePlace::Placeholder) => true,
        (&PatternValuePlace::Variable(_), _) => false,
        (_, &PatternValuePlace::Variable(_)) => false,
        (&PatternValuePlace::Placeholder, _) => false,
        (_, &PatternValuePlace::Placeholder) => false,
        _ => true,
    }
}
