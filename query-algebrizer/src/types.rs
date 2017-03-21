// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::fmt::{
    Debug,
    Formatter,
    Result,
};

use mentat_core::{
    Entid,
    TypedValue,
    ValueType,
};
/// This enum models the fixed set of default tables we have -- two
/// tables and two views.
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum DatomsTable {
    Datoms,             // The non-fulltext datoms table.
    FulltextValues,     // The virtual table mapping IDs to strings.
    FulltextDatoms,     // The fulltext-datoms view.
    AllDatoms,          // Fulltext and non-fulltext datoms.
}

impl DatomsTable {
    pub fn name(&self) -> &'static str {
        match *self {
            DatomsTable::Datoms => "datoms",
            DatomsTable::FulltextValues => "fulltext_values",
            DatomsTable::FulltextDatoms => "fulltext_datoms",
            DatomsTable::AllDatoms => "all_datoms",
        }
    }
}

/// One of the named columns of our tables.
#[derive(PartialEq, Eq, Clone, Debug)]
pub enum DatomsColumn {
    Entity,
    Attribute,
    Value,
    Tx,
    ValueTypeTag,
}

impl DatomsColumn {
    pub fn as_str(&self) -> &'static str {
        use self::DatomsColumn::*;
        match *self {
            Entity => "e",
            Attribute => "a",
            Value => "v",
            Tx => "tx",
            ValueTypeTag => "value_type_tag",
        }
    }
}

/// A specific instance of a table within a query. E.g., "datoms123".
pub type TableAlias = String;

/// The association between a table and its alias. E.g., AllDatoms, "all_datoms123".
#[derive(PartialEq, Eq, Clone)]
pub struct SourceAlias(pub DatomsTable, pub TableAlias);

impl Debug for SourceAlias {
    fn fmt(&self, f: &mut Formatter) -> Result {
        write!(f, "SourceAlias({:?}, {})", self.0, self.1)
    }
}

/// A particular column of a particular aliased table. E.g., "datoms123", Attribute.
#[derive(PartialEq, Eq, Clone)]
pub struct QualifiedAlias(pub TableAlias, pub DatomsColumn);

impl Debug for QualifiedAlias {
    fn fmt(&self, f: &mut Formatter) -> Result {
        write!(f, "{}.{}", self.0, self.1.as_str())
    }
}

impl QualifiedAlias {
    pub fn for_type_tag(&self) -> QualifiedAlias {
        QualifiedAlias(self.0.clone(), DatomsColumn::ValueTypeTag)
    }
}

#[derive(PartialEq, Eq)]
pub enum QueryValue {
    Column(QualifiedAlias),
    Entid(Entid),
    TypedValue(TypedValue),

    // This is different: a numeric value can only apply to the 'v' column, and it implicitly
    // constrains the `value_type_tag` column. For instance, a primitive long on `datoms00` of `5`
    // cannot be a boolean, so `datoms00.value_type_tag` must be in the set `#{0, 4, 5}`.
    // Note that `5 = 5.0` in SQLite, and we preserve that here.
    PrimitiveLong(i64),
}

impl Debug for QueryValue {
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        use self::QueryValue::*;
        match self {
            &Column(ref qa) => {
                write!(f, "{:?}", qa)
            },
            &Entid(ref entid) => {
                write!(f, "entity({:?})", entid)
            },
            &TypedValue(ref typed_value) => {
                write!(f, "value({:?})", typed_value)
            },
            &PrimitiveLong(value) => {
                write!(f, "primitive({:?})", value)
            },

        }
    }
}

#[derive(Copy, Clone, PartialEq, Eq)]
/// Define the different numeric inequality operators that we support.
/// Note that we deliberately don't just use "<=" and friends as strings:
/// Datalog and SQL don't use the same operators (e.g., `<>` and `!=`).
pub enum NumericComparison {
    LessThan,
    LessThanOrEquals,
    GreaterThan,
    GreaterThanOrEquals,
    NotEquals,
}

impl NumericComparison {
    pub fn to_sql_operator(self) -> &'static str {
        use self::NumericComparison::*;
        match self {
            LessThan            => "<",
            LessThanOrEquals    => "<=",
            GreaterThan         => ">",
            GreaterThanOrEquals => ">=",
            NotEquals           => "<>",
        }
    }

    pub fn from_datalog_operator(s: &str) -> Option<NumericComparison> {
        match s {
            "<"  => Some(NumericComparison::LessThan),
            "<=" => Some(NumericComparison::LessThanOrEquals),
            ">"  => Some(NumericComparison::GreaterThan),
            ">=" => Some(NumericComparison::GreaterThanOrEquals),
            "!=" => Some(NumericComparison::NotEquals),
            _    => None,
        }
    }
}

impl Debug for NumericComparison {
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        use self::NumericComparison::*;
        f.write_str(match self {
            &LessThan => "<",
            &LessThanOrEquals => "<=",
            &GreaterThan => ">",
            &GreaterThanOrEquals => ">=",
            &NotEquals => "!=",                // Datalog uses !=. SQL uses <>.
        })
    }
}

#[derive(PartialEq, Eq)]
pub enum ColumnConstraint {
    Equals(QualifiedAlias, QueryValue),
    NumericInequality {
        operator: NumericComparison,
        left: QueryValue,
        right: QueryValue,
    },
    HasType(TableAlias, ValueType),
}

impl Debug for ColumnConstraint {
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        use self::ColumnConstraint::*;
        match self {
            &Equals(ref qa1, ref thing) => {
                write!(f, "{:?} = {:?}", qa1, thing)
            },

            &NumericInequality { operator, ref left, ref right } => {
                write!(f, "{:?} {:?} {:?}", left, operator, right)
            },

            &HasType(ref qa, value_type) => {
                write!(f, "{:?}.value_type_tag = {:?}", qa, value_type)
            },
        }
    }
}
