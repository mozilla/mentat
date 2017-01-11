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

// TODO: understand why this is self::edn rather than just edn.
use self::edn::types::Value;

#[derive(Clone, Debug, PartialEq)]
pub enum EntId {
    EntId(i64),
    Ident(String),
}

#[derive(Clone, Debug, PartialEq)]
pub struct LookupRef {
    pub a: EntId,
    // TODO: consider boxing to allow recursive lookup refs.
    pub v: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub enum EntIdOrLookupRef {
    EntId(EntId),
    LookupRef(LookupRef),
}

#[derive(Clone, Debug, PartialEq)]
pub enum ValueOrLookupRef {
    Value(Value),
    LookupRef(LookupRef),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Entity {
    Add {
        e: EntIdOrLookupRef,
        a: EntId,
        v: ValueOrLookupRef,
        tx: Option<EntId>,
    },
    Retract {
        e: EntIdOrLookupRef,
        a: EntId,
        v: ValueOrLookupRef,
    },
    RetractAttribute { e: EntIdOrLookupRef, a: EntId },
    RetractEntity { e: EntIdOrLookupRef },
}
