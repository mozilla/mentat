// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::BTreeSet;
use std::fmt::{
    Debug,
    Formatter,
    Result,
};

use enum_set::{
    CLike,
    EnumSet,
};

use mentat_core::{
    Entid,
    TypedValue,
    ValueType,
};

use mentat_query::{
    Direction,
    NamespacedKeyword,
    Order,
    Variable,
};

/// This enum models the fixed set of default tables we have -- two
/// tables and two views -- and computed tables defined in the enclosing CC.
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum DatomsTable {
    Datoms,             // The non-fulltext datoms table.
    FulltextValues,     // The virtual table mapping IDs to strings.
    FulltextDatoms,     // The fulltext-datoms view.
    AllDatoms,          // Fulltext and non-fulltext datoms.
    Computed(usize),    // A computed table, tracked elsewhere in the query.
}

/// A source of rows that isn't a named table -- typically a subquery or union.
#[derive(PartialEq, Eq, Debug)]
pub enum ComputedTable {
    Subquery(::clauses::ConjoiningClauses),
    Union {
        projection: BTreeSet<Variable>,
        type_extraction: BTreeSet<Variable>,
        arms: Vec<::clauses::ConjoiningClauses>,
    },
    NamedValues {
        names: Vec<Variable>,
        values: Vec<TypedValue>,
    },
}

impl DatomsTable {
    pub fn name(&self) -> &'static str {
        match *self {
            DatomsTable::Datoms => "datoms",
            DatomsTable::FulltextValues => "fulltext_values",
            DatomsTable::FulltextDatoms => "fulltext_datoms",
            DatomsTable::AllDatoms => "all_datoms",
            DatomsTable::Computed(_) => "c",
        }
    }
}

pub trait ColumnName {
    fn column_name(&self) -> String;
}

/// One of the named columns of our tables.
#[derive(PartialEq, Eq, Clone)]
pub enum DatomsColumn {
    Entity,
    Attribute,
    Value,
    Tx,
    ValueTypeTag,
}

#[derive(PartialEq, Eq, Clone)]
pub enum VariableColumn {
    Variable(Variable),
    VariableTypeTag(Variable),
}

#[derive(PartialEq, Eq, Clone)]
pub enum Column {
    Fixed(DatomsColumn),
    Variable(VariableColumn),
}

impl From<DatomsColumn> for Column {
    fn from(from: DatomsColumn) -> Column {
        Column::Fixed(from)
    }
}

impl From<VariableColumn> for Column {
    fn from(from: VariableColumn) -> Column {
        Column::Variable(from)
    }
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

impl ColumnName for DatomsColumn {
    fn column_name(&self) -> String {
        self.as_str().to_string()
    }
}

impl ColumnName for VariableColumn {
    fn column_name(&self) -> String {
        match self {
            &VariableColumn::Variable(ref v) => v.to_string(),
            &VariableColumn::VariableTypeTag(ref v) => format!("{}_value_type_tag", v.as_str()),
        }
    }
}

impl Debug for VariableColumn {
    fn fmt(&self, f: &mut Formatter) -> Result {
        match self {
            // These should agree with VariableColumn::column_name.
            &VariableColumn::Variable(ref v) => write!(f, "{}", v.as_str()),
            &VariableColumn::VariableTypeTag(ref v) => write!(f, "{}_value_type_tag", v.as_str()),
        }
    }
}

impl Debug for DatomsColumn {
    fn fmt(&self, f: &mut Formatter) -> Result {
        write!(f, "{}", self.as_str())
    }
}

impl Debug for Column {
    fn fmt(&self, f: &mut Formatter) -> Result {
        match self {
            &Column::Fixed(ref c) => c.fmt(f),
            &Column::Variable(ref v) => v.fmt(f),
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
pub struct QualifiedAlias(pub TableAlias, pub Column);

impl Debug for QualifiedAlias {
    fn fmt(&self, f: &mut Formatter) -> Result {
        write!(f, "{}.{:?}", self.0, self.1)
    }
}

impl QualifiedAlias {
    pub fn new<C: Into<Column>>(table: TableAlias, column: C) -> Self {
        QualifiedAlias(table, column.into())
    }

    pub fn for_type_tag(&self) -> QualifiedAlias {
        // TODO: this only makes sense for `DatomsColumn` tables.
        QualifiedAlias(self.0.clone(), Column::Fixed(DatomsColumn::ValueTypeTag))
    }
}

#[derive(PartialEq, Eq, Clone)]
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

/// Represents an entry in the ORDER BY list: a variable or a variable's type tag.
/// (We require order vars to be projected, so we can simply use a variable here.)
#[derive(Debug)]
pub struct OrderBy(pub Direction, pub VariableColumn);

impl From<Order> for OrderBy {
    fn from(item: Order) -> OrderBy {
        let Order(direction, variable) = item;
        OrderBy(direction, VariableColumn::Variable(variable))
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
    NotExists(ComputedTable),
}

#[derive(PartialEq, Eq, Debug)]
pub enum ColumnConstraintOrAlternation {
    Constraint(ColumnConstraint),
    Alternation(ColumnAlternation),
}

impl From<ColumnConstraint> for ColumnConstraintOrAlternation {
    fn from(thing: ColumnConstraint) -> Self {
        ColumnConstraintOrAlternation::Constraint(thing)
    }
}

/// A `ColumnIntersection` constraint is satisfied if all of its inner constraints are satisfied.
/// An empty intersection is always satisfied.
#[derive(PartialEq, Eq)]
pub struct ColumnIntersection(pub Vec<ColumnConstraintOrAlternation>);

impl From<Vec<ColumnConstraint>> for ColumnIntersection {
    fn from(thing: Vec<ColumnConstraint>) -> Self {
        ColumnIntersection(thing.into_iter().map(|x| x.into()).collect())
    }
}

impl Default for ColumnIntersection {
    fn default() -> Self {
        ColumnIntersection(vec![])
    }
}

impl IntoIterator for ColumnIntersection {
    type Item = ColumnConstraintOrAlternation;
    type IntoIter = ::std::vec::IntoIter<ColumnConstraintOrAlternation>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl ColumnIntersection {
    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[inline]
    pub fn add(&mut self, constraint: ColumnConstraintOrAlternation) {
        self.0.push(constraint);
    }

    #[inline]
    pub fn add_intersection(&mut self, constraint: ColumnConstraint) {
        self.add(ColumnConstraintOrAlternation::Constraint(constraint));
    }

    pub fn append(&mut self, other: &mut Self) {
        self.0.append(&mut other.0)
    }
}

/// A `ColumnAlternation` constraint is satisfied if at least one of its inner constraints is
/// satisfied. An empty `ColumnAlternation` is never satisfied.
#[derive(PartialEq, Eq, Debug)]
pub struct ColumnAlternation(pub Vec<ColumnIntersection>);

impl Default for ColumnAlternation {
    fn default() -> Self {
        ColumnAlternation(vec![])
    }
}

impl IntoIterator for ColumnAlternation {
    type Item = ColumnIntersection;
    type IntoIter = ::std::vec::IntoIter<ColumnIntersection>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl ColumnAlternation {
    pub fn add_alternate(&mut self, intersection: ColumnIntersection) {
        self.0.push(intersection);
    }
}

impl Debug for ColumnIntersection {
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
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
            &NotExists(ref ct) => {
                write!(f, "NOT EXISTS {:?}", ct)
            },
        }
    }
}

#[derive(PartialEq, Clone)]
pub enum EmptyBecause {
    /// Var, existing, desired.
    ConflictingBindings(Variable, TypedValue, TypedValue),

    /// Var, existing, desired.
    TypeMismatch(Variable, ValueTypeSet, ValueTypeSet),
    NoValidTypes(Variable),
    NonNumericArgument,
    NonStringFulltextValue,
    UnresolvedIdent(NamespacedKeyword),
    InvalidAttributeIdent(NamespacedKeyword),
    InvalidAttributeEntid(Entid),
    InvalidBinding(Column, TypedValue),
    ValueTypeMismatch(ValueType, TypedValue),
    AttributeLookupFailed,         // Catch-all, because the table lookup code is lazy. TODO
}

impl Debug for EmptyBecause {
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        use self::EmptyBecause::*;
        match self {
            &ConflictingBindings(ref var, ref existing, ref desired) => {
                write!(f, "Var {:?} can't be bound to both {:?} and {:?}",
                       var, desired, existing)
            },
            &TypeMismatch(ref var, ref existing, ref desired) => {
                write!(f, "Type mismatch: {:?} can't be {:?}, because it's already {:?}",
                       var, desired, existing)
            },
            &NoValidTypes(ref var) => {
                write!(f, "Type mismatch: {:?} has no valid types", var)
            },
            &NonNumericArgument => {
                write!(f, "Non-numeric argument in numeric place")
            },
            &NonStringFulltextValue => {
                write!(f, "Non-string argument for fulltext attribute")
            },
            &UnresolvedIdent(ref kw) => {
                write!(f, "Couldn't resolve keyword {}", kw)
            },
            &InvalidAttributeIdent(ref kw) => {
                write!(f, "{} does not name an attribute", kw)
            },
            &InvalidAttributeEntid(entid) => {
                write!(f, "{} is not an attribute", entid)
            },
            &InvalidBinding(ref column, ref tv) => {
                write!(f, "{:?} cannot name column {:?}", tv, column)
            },
            &ValueTypeMismatch(value_type, ref typed_value) => {
                write!(f, "Type mismatch: {:?} doesn't match attribute type {:?}",
                       typed_value, value_type)
            },
            &AttributeLookupFailed => {
                write!(f, "Attribute lookup failed")
            },
        }
    }
}

trait EnumSetExtensions<T: CLike + Clone> {
    /// Return a set containing both `x` and `y`.
    fn of_both(x: T, y: T) -> EnumSet<T>;

    /// Return a clone of `self` with `y` added.
    fn with(&self, y: T) -> EnumSet<T>;
}

impl<T: CLike + Clone> EnumSetExtensions<T> for EnumSet<T> {
    /// Return a set containing both `x` and `y`.
    fn of_both(x: T, y: T) -> Self {
        let mut o = EnumSet::new();
        o.insert(x);
        o.insert(y);
        o
    }

    /// Return a clone of `self` with `y` added.
    fn with(&self, y: T) -> EnumSet<T> {
        let mut o = self.clone();
        o.insert(y);
        o
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ValueTypeSet(pub EnumSet<ValueType>);

impl Default for ValueTypeSet {
    fn default() -> ValueTypeSet {
        ValueTypeSet::any()
    }
}

impl ValueTypeSet {
    pub fn any() -> ValueTypeSet {
        ValueTypeSet(ValueType::all_enums())
    }

    pub fn none() -> ValueTypeSet {
        ValueTypeSet(EnumSet::new())
    }

    /// Return a set containing only `t`.
    pub fn of_one(t: ValueType) -> ValueTypeSet {
        let mut s = EnumSet::new();
        s.insert(t);
        ValueTypeSet(s)
    }

    /// Return a set containing `Double` and `Long`.
    pub fn of_numeric_types() -> ValueTypeSet {
        ValueTypeSet(EnumSet::of_both(ValueType::Double, ValueType::Long))
    }

    /// Return a set containing `Ref` and `Keyword`.
    pub fn of_keywords() -> ValueTypeSet {
        ValueTypeSet(EnumSet::of_both(ValueType::Ref, ValueType::Keyword))
    }

    /// Return a set containing `Ref` and `Long`.
    pub fn of_longs() -> ValueTypeSet {
        ValueTypeSet(EnumSet::of_both(ValueType::Ref, ValueType::Long))
    }
}

impl ValueTypeSet {
    pub fn insert(&mut self, vt: ValueType) -> bool {
        self.0.insert(vt)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns a set containing all the types in this set and `other`.
    pub fn union(&self, other: &ValueTypeSet) -> ValueTypeSet {
        ValueTypeSet(self.0.union(other.0))
    }

    pub fn intersection(&self, other: &ValueTypeSet) -> ValueTypeSet {
        ValueTypeSet(self.0.intersection(other.0))
    }

    /// Return an arbitrary type that's part of this set.
    /// For a set containing a single type, this will be that type.
    pub fn exemplar(&self) -> Option<ValueType> {
        self.0.iter().next()
    }

    pub fn contains(&self, vt: ValueType) -> bool {
        self.0.contains(&vt)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn is_unit(&self) -> bool {
        self.0.len() == 1
    }
}
