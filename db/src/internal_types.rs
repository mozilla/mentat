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

use std;
use std::collections::HashMap;
use std::rc::Rc;

use errors;
use errors::ErrorKind;
use types::*;
use mentat_tx::entities::OpType;

#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub enum Term<E, V> {
    AddOrRetract(OpType, E, Entid, V),
}

pub type EntidOr<T> = std::result::Result<Entid, T>;
pub type TypedValueOr<T> = std::result::Result<TypedValue, T>;

pub type TempId = Rc<String>;
pub type TempIdMap = HashMap<TempId, Entid>;

pub type LookupRef = Rc<AVPair>;

/// Internal representation of an entid on its way to resolution.  We either have the simple case (a
/// numeric entid), a lookup-ref that still needs to be resolved (an atomized [a v] pair), or a temp
/// ID that needs to be upserted or allocated (an atomized tempid).
#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub enum LookupRefOrTempId {
    LookupRef(LookupRef),
    TempId(TempId)
}

pub type TermWithTempIdsAndLookupRefs = Term<EntidOr<LookupRefOrTempId>, TypedValueOr<LookupRefOrTempId>>;
pub type TermWithTempIds = Term<EntidOr<TempId>, TypedValueOr<TempId>>;
pub type TermWithoutTempIds = Term<Entid, TypedValue>;
pub type Population = Vec<TermWithTempIds>;

impl TermWithTempIds {
    // These have no tempids by definition, and just need to be unwrapped.  This operation might
    // also be called "lowering" or "level lowering", but the concept of "unwrapping" is common in
    // Rust and seems appropriate here.
    pub fn unwrap(self) -> TermWithoutTempIds {
        match self {
            Term::AddOrRetract(op, Ok(n), a, Ok(v)) => Term::AddOrRetract(op, n, a, v),
            _ => unreachable!(),
        }
    }
}

/// Given an `EntidOr` or a `TypedValueOr`, replace any internal `LookupRef` with the entid from
/// the given map.  Fail if any `LookupRef` cannot be replaced.
///
/// `lift` allows to specify how the entid found is mapped into the output type.  (This could
/// also be an `Into` or `From` requirement.)
///
/// The reason for this awkward expression is that we're parameterizing over the _type constructor_
/// (`EntidOr` or `TypedValueOr`), which is not trivial to express in Rust.  This only works because
/// they're both the same `Result<...>` type with different parameterizations.
pub fn replace_lookup_ref<T, U>(lookup_map: &AVMap, desired_or: Result<T, LookupRefOrTempId>, lift: U) -> errors::Result<Result<T, TempId>> where U: FnOnce(Entid) -> T {
    match desired_or {
        Ok(desired) => Ok(Ok(desired)), // N.b., must unwrap here -- the ::Ok types are different!
        Err(other) => {
            match other {
                LookupRefOrTempId::TempId(t) => Ok(Err(t)),
                LookupRefOrTempId::LookupRef(av) => lookup_map.get(&*av)
                    .map(|x| lift(*x)).map(Ok)
                    // XXX TODO: fix this error kind!
                    .ok_or_else(|| ErrorKind::UnrecognizedIdent(format!("couldn't lookup [a v]: {:?}", (*av).clone())).into()),
            }
        }
    }
}
