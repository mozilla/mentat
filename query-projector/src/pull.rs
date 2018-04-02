// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::{
    BTreeSet,
};

use indexmap::{
    IndexMap,
    IndexSet,
};

use mentat_core::{
    Entid,
    SQLValueType,
    SQLValueTypeSet,
    TypedValue,
    ValueType,
    ValueTypeSet,
};

use mentat_core::util::{
    Either,
};

use mentat_query::{
    Element,
    Variable,
};

use mentat_query_algebrizer::{
    AlgebraicQuery,
    ColumnName,
    ConjoiningClauses,
    QualifiedAlias,
    VariableColumn,
};


use mentat_query_sql::{
    ColumnOrExpression,
    GroupBy,
    Name,
    Projection,
    ProjectedColumn,
};

use aggregates::{
    SimpleAggregation,
    projected_column_for_simple_aggregate,
};

use errors::{
    ErrorKind,
    Result,
};

use super::{
    TypedIndex,
};

/// A pull expression expands a binding into a structure. The returned structure
/// associates attributes named in the input or retrieved from the store with values.
/// This association is a `StructuredMap`.
struct StructuredMap {
    attrs: IndexMap<Entid, StructuredValue>,
}

/// The values stored in a `StructuredMap` can be:
/// * Vecs of structured values, for multi-valued component attributes or nested expressions.
/// * Vecs of typed values, for multi-valued simple attributes. Unlike Datomic, Mentat can express
///   an entity without a `{:db/id 12345678}` map.
/// * Single structured values, for single-valued component attributes or nested expressions.
/// * Single typed values, for simple attributes.
enum StructuredValue {
    Value(TypedValue),
    Values(Vec<TypedValue>),
    Structure(StructuredMap),
    Structures(Vec<StructuredMap>),
}
