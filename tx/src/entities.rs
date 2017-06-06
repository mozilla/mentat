// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

///! This module defines core types that support the transaction processor.

extern crate edn;

use std::collections::BTreeMap;
use std::fmt;

use self::edn::symbols::NamespacedKeyword;

/// A tempid, either an external tempid given in a transaction (usually as an `edn::Value::Text`),
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

    pub fn into_internal(self) -> Option<i64> {
        match self {
            TempId::Internal(x) => Some(x),
            TempId::External(_) => None,
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
pub enum Entid {
    Entid(i64),
    Ident(NamespacedKeyword),
}

impl Entid {
    pub fn is_backward(&self) -> bool {
        match self {
            &Entid::Entid(_) => false,
            &Entid::Ident(ref a) => a.is_backward(),
        }
    }

    pub fn to_reversed(&self) -> Option<Entid> {
        match self {
            &Entid::Entid(_) => None,
            &Entid::Ident(ref a) => Some(Entid::Ident(a.to_reversed())),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub struct LookupRef {
    pub a: Entid,
    // In theory we could allow nested lookup-refs.  In practice this would require us to process
    // lookup-refs in multiple phases, like how we resolve tempids, which isn't worth the effort.
    pub v: edn::Value, // An atom.
}

pub type MapNotation = BTreeMap<Entid, AtomOrLookupRefOrVectorOrMapNotation>;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum AtomOrLookupRefOrVectorOrMapNotation {
    Atom(edn::ValueAndSpan),
    LookupRef(LookupRef),
    Vector(Vec<AtomOrLookupRefOrVectorOrMapNotation>),
    MapNotation(MapNotation),
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum EntidOrLookupRefOrTempId {
    Entid(Entid),
    LookupRef(LookupRef),
    TempId(TempId),
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum OpType {
    Add,
    Retract,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum Entity {
    // Like [:db/add|:db/retract e a v].
    AddOrRetract {
        op: OpType,
        e: EntidOrLookupRefOrTempId,
        a: Entid,
        v: AtomOrLookupRefOrVectorOrMapNotation,
    },
    // Like {:db/id "tempid" a1 v1 a2 v2}.
    MapNotation(MapNotation),
}
