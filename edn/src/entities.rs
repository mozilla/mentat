// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

//! This module defines core types that support the transaction processor.

use std::collections::BTreeMap;
use std::fmt;

use value_rc::{
    ValueRc,
};

use symbols::{
    Keyword,
    PlainSymbol,
};

use types::{
    ValueAndSpan,
};

/// `EntityPlace` and `ValuePlace` embed values, either directly (i.e., `ValuePlace::Atom`) or
/// indirectly (i.e., `EntityPlace::LookupRef`).  In order to maintain the graph of `Into` and
/// `From` relations, we need to ensure that `{Value,Entity}Place` can't match as a potential value.
/// (If it does, the `impl Into<T> for T` default conflicts.) This marker trait allows to mark
/// acceptable values, thereby removing `{Entity,Value}Place` from consideration.
pub trait TransactableValueMarker {}

/// `ValueAndSpan` is the value type coming out of the entity parser.
impl TransactableValueMarker for ValueAndSpan {}

/// A tempid, either an external tempid given in a transaction (usually as an `Value::Text`),
/// or an internal tempid allocated by Mentat itself.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum TempId {
    External(String),
    Internal(i64),
}

impl TempId {
    pub fn into_external(self) -> Option<String> {
        match self {
            TempId::External(s) => Some(s),
            TempId::Internal(_) => None,
        }
    }
}

impl fmt::Display for TempId {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            &TempId::External(ref s) => write!(f, "{}", s),
            &TempId::Internal(x) => write!(f, "<tempid {}>", x),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum EntidOrIdent {
    Entid(i64),
    Ident(Keyword),
}

impl From<i64> for EntidOrIdent {
    fn from(v: i64) -> Self {
        EntidOrIdent::Entid(v)
    }
}

impl From<Keyword> for EntidOrIdent {
    fn from(v: Keyword) -> Self {
        EntidOrIdent::Ident(v)
    }
}

impl EntidOrIdent {
    pub fn unreversed(&self) -> Option<EntidOrIdent> {
        match self {
            &EntidOrIdent::Entid(_) => None,
            &EntidOrIdent::Ident(ref a) => a.unreversed().map(EntidOrIdent::Ident),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub struct LookupRef<V> {
    pub a: AttributePlace,
    // In theory we could allow nested lookup-refs.  In practice this would require us to process
    // lookup-refs in multiple phases, like how we resolve tempids, which isn't worth the effort.
    pub v: V, // An atom.
}

/// A "transaction function" that exposes some value determined by the current transaction.  The
/// prototypical example is the current transaction ID, `(transaction-tx)`.
///
/// A natural next step might be to expose the current transaction instant `(transaction-instant)`,
/// but that's more difficult: the transaction itself can set the transaction instant (with some
/// restrictions), so the transaction function must be late-binding.  Right now, that's difficult to
/// arrange in the transactor.
///
/// In the future, we might accept arguments; for example, perhaps we might expose `(ancestor
/// (transaction-tx) n)` to find the n-th ancestor of the current transaction.  If we do accept
/// arguments, then the special case of `(lookup-ref a v)` should be handled as part of the
/// generalization.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub struct TxFunction {
    pub op: PlainSymbol,
}

pub type MapNotation<V> = BTreeMap<EntidOrIdent, ValuePlace<V>>;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum ValuePlace<V> {
    // We never know at parse-time whether an integer or ident is really an entid, but we will often
    // know when building entities programmatically.
    Entid(EntidOrIdent),
    // We never know at parse-time whether a string is really a tempid, but we will often know when
    // building entities programmatically.
    TempId(ValueRc<TempId>),
    LookupRef(LookupRef<V>),
    TxFunction(TxFunction),
    Vector(Vec<ValuePlace<V>>),
    Atom(V),
    MapNotation(MapNotation<V>),
}

impl<V: TransactableValueMarker> From<EntidOrIdent> for ValuePlace<V> {
    fn from(v: EntidOrIdent) -> Self {
        ValuePlace::Entid(v)
    }
}

impl<V: TransactableValueMarker> From<TempId> for ValuePlace<V> {
    fn from(v: TempId) -> Self {
        ValuePlace::TempId(v.into())
    }
}

impl<V: TransactableValueMarker> From<ValueRc<TempId>> for ValuePlace<V> {
    fn from(v: ValueRc<TempId>) -> Self {
        ValuePlace::TempId(v)
    }
}

impl<V: TransactableValueMarker> From<LookupRef<V>> for ValuePlace<V> {
    fn from(v: LookupRef<V>) -> Self {
        ValuePlace::LookupRef(v)
    }
}

impl<V: TransactableValueMarker> From<TxFunction> for ValuePlace<V> {
    fn from(v: TxFunction) -> Self {
        ValuePlace::TxFunction(v)
    }
}

impl<V: TransactableValueMarker> From<Vec<ValuePlace<V>>> for ValuePlace<V> {
    fn from(v: Vec<ValuePlace<V>>) -> Self {
        ValuePlace::Vector(v)
    }
}

impl<V: TransactableValueMarker> From<V> for ValuePlace<V> {
    fn from(v: V) -> Self {
        ValuePlace::Atom(v)
    }
}

impl<V: TransactableValueMarker> From<MapNotation<V>> for ValuePlace<V> {
    fn from(v: MapNotation<V>) -> Self {
        ValuePlace::MapNotation(v)
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum EntityPlace<V> {
    Entid(EntidOrIdent),
    TempId(ValueRc<TempId>),
    LookupRef(LookupRef<V>),
    TxFunction(TxFunction),
}

impl<V, E: Into<EntidOrIdent>> From<E> for EntityPlace<V> {
    fn from(v: E) -> Self {
        EntityPlace::Entid(v.into())
    }
}

impl<V: TransactableValueMarker> From<TempId> for EntityPlace<V> {
    fn from(v: TempId) -> Self {
        EntityPlace::TempId(v.into())
    }
}

impl<V: TransactableValueMarker> From<ValueRc<TempId>> for EntityPlace<V> {
    fn from(v: ValueRc<TempId>) -> Self {
        EntityPlace::TempId(v)
    }
}

impl<V: TransactableValueMarker> From<LookupRef<V>> for EntityPlace<V> {
    fn from(v: LookupRef<V>) -> Self {
        EntityPlace::LookupRef(v)
    }
}

impl<V: TransactableValueMarker> From<TxFunction> for EntityPlace<V> {
    fn from(v: TxFunction) -> Self {
        EntityPlace::TxFunction(v)
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum AttributePlace {
    Entid(EntidOrIdent),
}

impl<A: Into<EntidOrIdent>> From<A> for AttributePlace {
    fn from(v: A) -> Self {
        AttributePlace::Entid(v.into())
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum OpType {
    Add,
    Retract,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum Entity<V> {
    // Like [:db/add|:db/retract e a v].
    AddOrRetract {
        op: OpType,
        e: EntityPlace<V>,
        a: AttributePlace,
        v: ValuePlace<V>,
    },
    // Like {:db/id "tempid" a1 v1 a2 v2}.
    MapNotation(MapNotation<V>),
}
