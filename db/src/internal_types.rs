// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]

//! Types used only within the transactor.  These should not be exposed outside of this crate.

use std::collections::{
    BTreeMap,
    BTreeSet,
    HashMap,
};
use std::rc::Rc;

use mentat_core::KnownEntid;

use mentat_core::util::Either;

use edn;
use edn::{
    SpannedValue,
    ValueAndSpan,
};

use errors;
use errors::{
    ErrorKind,
    Result,
};
use schema::{
    SchemaTypeChecking,
};
use types::{
    Attribute,
    AVMap,
    AVPair,
    Entid,
    Schema,
    TypedValue,
    ValueType,
};
use mentat_tx::entities;
use mentat_tx::entities::{
    EntidOrLookupRefOrTempId,
    OpType,
    TempId,
    TxFunction,
};

/// The transactor is tied to `edn::ValueAndSpan` right now, but in the future we'd like to support
/// `TypedValue` directly for programmatic use.  `TransactableValue` encapsulates the interface
/// value types (i.e., values in the value place) need to support to be transacted.
pub trait TransactableValue {
    /// Coerce this value place into the given type.  This is where we perform schema-aware
    /// coercion, for example coercing an integral value into a ref where appropriate.
    fn into_typed_value(self, schema: &Schema, value_type: ValueType) -> Result<TypedValue>;

    /// Make an entity place out of this value place.  This is where we limit values in nested maps
    /// to valid entity places.
    fn into_entity_place(self) -> Result<EntidOrLookupRefOrTempId>;

    fn as_tempid(&self) -> Option<TempId>;
}

impl TransactableValue for ValueAndSpan {
    fn into_typed_value(self, schema: &Schema, value_type: ValueType) -> Result<TypedValue> {
        schema.to_typed_value(&self.without_spans(), value_type)
    }

    fn into_entity_place(self) -> Result<EntidOrLookupRefOrTempId> {
        use self::SpannedValue::*;
        match self.inner {
            Integer(v) => Ok(EntidOrLookupRefOrTempId::Entid(entities::Entid::Entid(v))),
            Keyword(v) => {
                if v.is_namespaced() {
                    Ok(EntidOrLookupRefOrTempId::Entid(entities::Entid::Ident(v)))
                } else {
                    // We only allow namespaced idents.
                    bail!(ErrorKind::InputError(errors::InputError::BadEntityPlace))
                }
            },
            Text(v) => Ok(EntidOrLookupRefOrTempId::TempId(TempId::External(v))),
            List(ls) => {
                let mut it = ls.iter();
                match (it.next().map(|x| &x.inner), it.next(), it.next(), it.next()) {
                    // Like "(transaction-id)".
                    (Some(&PlainSymbol(ref op)), None, None, None) => {
                        Ok(EntidOrLookupRefOrTempId::TxFunction(TxFunction { op: op.clone() }))
                    },
                    // Like "(lookup-ref)".
                    (Some(&PlainSymbol(edn::PlainSymbol(ref s))), Some(a), Some(v), None) if s == "lookup-ref" => {
                        match a.clone().into_entity_place()? {
                            EntidOrLookupRefOrTempId::Entid(a) => Ok(EntidOrLookupRefOrTempId::LookupRef(entities::LookupRef { a: entities::AttributePlace::Entid(a), v: v.clone().without_spans() })),
                            EntidOrLookupRefOrTempId::TempId(_) |
                            EntidOrLookupRefOrTempId::TxFunction(_) |
                            EntidOrLookupRefOrTempId::LookupRef(_) => bail!(ErrorKind::InputError(errors::InputError::BadEntityPlace)),
                        }
                    },
                    _ => bail!(ErrorKind::InputError(errors::InputError::BadEntityPlace)),
                }
            },
            Nil |
            Boolean(_) |
            Instant(_) |
            BigInteger(_) |
            Float(_) |
            Uuid(_) |
            PlainSymbol(_) |
            NamespacedSymbol(_) |
            Vector(_) |
            Set(_) |
            Map(_) => bail!(ErrorKind::InputError(errors::InputError::BadEntityPlace)),
        }
    }

    fn as_tempid(&self) -> Option<TempId> {
        self.inner.as_text().cloned().map(TempId::External)
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum Term<E, V> {
    AddOrRetract(OpType, E, Entid, V),
}

use self::Either::*;

pub type KnownEntidOr<T> = Either<KnownEntid, T>;
pub type TypedValueOr<T> = Either<TypedValue, T>;

pub type TempIdHandle = Rc<TempId>;
pub type TempIdMap = HashMap<TempIdHandle, KnownEntid>;

pub type LookupRef = Rc<AVPair>;

/// Internal representation of an entid on its way to resolution.  We either have the simple case (a
/// numeric entid), a lookup-ref that still needs to be resolved (an atomized [a v] pair), or a temp
/// ID that needs to be upserted or allocated (an atomized tempid).
#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub enum LookupRefOrTempId {
    LookupRef(LookupRef),
    TempId(TempIdHandle)
}

pub type TermWithTempIdsAndLookupRefs = Term<KnownEntidOr<LookupRefOrTempId>, TypedValueOr<LookupRefOrTempId>>;
pub type TermWithTempIds = Term<KnownEntidOr<TempIdHandle>, TypedValueOr<TempIdHandle>>;
pub type TermWithoutTempIds = Term<KnownEntid, TypedValue>;
pub type Population = Vec<TermWithTempIds>;

impl TermWithTempIds {
    // These have no tempids by definition, and just need to be unwrapped.  This operation might
    // also be called "lowering" or "level lowering", but the concept of "unwrapping" is common in
    // Rust and seems appropriate here.
    pub(crate) fn unwrap(self) -> TermWithoutTempIds {
        match self {
            Term::AddOrRetract(op, Left(n), a, Left(v)) => Term::AddOrRetract(op, n, a, v),
            _ => unreachable!(),
        }
    }
}

/// Given a `KnownEntidOr` or a `TypedValueOr`, replace any internal `LookupRef` with the entid from
/// the given map.  Fail if any `LookupRef` cannot be replaced.
///
/// `lift` allows to specify how the entid found is mapped into the output type.  (This could
/// also be an `Into` or `From` requirement.)
///
/// The reason for this awkward expression is that we're parameterizing over the _type constructor_
/// (`EntidOr` or `TypedValueOr`), which is not trivial to express in Rust.  This only works because
/// they're both the same `Result<...>` type with different parameterizations.
pub fn replace_lookup_ref<T, U>(lookup_map: &AVMap, desired_or: Either<T, LookupRefOrTempId>, lift: U) -> errors::Result<Either<T, TempIdHandle>> where U: FnOnce(Entid) -> T {
    match desired_or {
        Left(desired) => Ok(Left(desired)), // N.b., must unwrap here -- the ::Left types are different!
        Right(other) => {
            match other {
                LookupRefOrTempId::TempId(t) => Ok(Right(t)),
                LookupRefOrTempId::LookupRef(av) => lookup_map.get(&*av)
                    .map(|x| lift(*x)).map(Left)
                    // XXX TODO: fix this error kind!
                    .ok_or_else(|| ErrorKind::UnrecognizedIdent(format!("couldn't lookup [a v]: {:?}", (*av).clone())).into()),
            }
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct AddAndRetract {
    pub(crate) add: BTreeSet<TypedValue>,
    pub(crate) retract: BTreeSet<TypedValue>,
}

// A trie-like structure mapping a -> e -> v that prefix compresses and makes uniqueness constraint
// checking more efficient.  BTree* for deterministic errors.
pub(crate) type AEVTrie<'schema> = BTreeMap<(Entid, &'schema Attribute), BTreeMap<Entid, AddAndRetract>>;
