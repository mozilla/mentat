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

use std::collections::{
    BTreeMap,
    BTreeSet,
};

use rusqlite;

use mentat_tx::entities::{
    TempId,
};
use mentat_core::{
    KnownEntid,
};
use types::{
    Entid,
    TypedValue,
    ValueType,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CardinalityConflict {
    /// A cardinality one attribute has multiple assertions `[e a v1], [e a v2], ...`.
    CardinalityOneAddConflict {
        e: Entid,
        a: Entid,
        vs: BTreeSet<TypedValue>,
    },

    /// A datom has been both asserted and retracted, like `[:db/add e a v]` and `[:db/retract e a v]`.
    AddRetractConflict {
        e: Entid,
        a: Entid,
        vs: BTreeSet<TypedValue>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SchemaConstraintViolation {
    /// A transaction tried to assert datoms where one tempid upserts to two (or more) distinct
    /// entids.
    ConflictingUpserts {
        /// A map from tempid to the entids it would upsert to.
        ///
        /// In the future, we might even be able to attribute the upserts to particular (reduced)
        /// datoms, i.e., to particular `[e a v]` triples that caused the constraint violation.
        /// Attributing constraint violations to input data is more difficult to the multiple
        /// rewriting passes the input undergoes.
        conflicting_upserts: BTreeMap<TempId, BTreeSet<KnownEntid>>,
    },

    /// A transaction tried to assert a datom or datoms with the wrong value `v` type(s).
    TypeDisagreements {
        /// The key (`[e a v]`) has an invalid value `v`: it is not of the expected value type.
        conflicting_datoms: BTreeMap<(Entid, Entid, TypedValue), ValueType>
    },

    /// A transaction tried to assert datoms that don't observe the schema's cardinality constraints.
    CardinalityConflicts {
        conflicts: Vec<CardinalityConflict>,
    },
}

impl ::std::fmt::Display for SchemaConstraintViolation {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        use self::SchemaConstraintViolation::*;
        match self {
            &ConflictingUpserts { ref conflicting_upserts } => {
                writeln!(f, "conflicting upserts:")?;
                for (tempid, entids) in conflicting_upserts {
                    writeln!(f, "  tempid {:?} upserts to {:?}", tempid, entids)?;
                }
                Ok(())
            },
            &TypeDisagreements { ref conflicting_datoms } => {
                writeln!(f, "type disagreements:")?;
                for (ref datom, expected_type) in conflicting_datoms {
                    writeln!(f, "  expected value of type {} but got datom [{} {} {:?}]", expected_type, datom.0, datom.1, datom.2)?;
                }
                Ok(())
            },
            &CardinalityConflicts { ref conflicts } => {
                writeln!(f, "cardinality conflicts:")?;
                for ref conflict in conflicts {
                    writeln!(f, "  {:?}", conflict)?;
                }
                Ok(())
            },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InputError {
    /// Map notation included a bad `:db/id` value.
    BadDbId,

    /// A value place cannot be interpreted as an entity place (for example, in nested map
    /// notation).
    BadEntityPlace,
}

impl ::std::fmt::Display for InputError {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        use self::InputError::*;
        match self {
            &BadDbId => {
                writeln!(f, ":db/id in map notation must either not be present or be an entid, an ident, or a tempid")
            },
            &BadEntityPlace => {
                writeln!(f, "cannot convert value place into entity place")
            },
        }
    }
}

error_chain! {
    types {
        Error, ErrorKind, ResultExt, Result;
    }

    foreign_links {
        Rusqlite(rusqlite::Error);
    }

    errors {
        /// We're just not done yet.  Message that the feature is recognized but not yet
        /// implemented.
        NotYetImplemented(t: String) {
            description("not yet implemented")
            display("not yet implemented: {}", t)
        }

        /// We've been given a value that isn't the correct Mentat type.
        BadValuePair(value: String, value_type: ValueType) {
            description("value is not the expected Mentat value type")
            display("value '{}' is not the expected Mentat value type {:?}", value, value_type)
        }

        /// We've got corrupt data in the SQL store: a value and value_type_tag don't line up.
        BadSQLValuePair(value: rusqlite::types::Value, value_type_tag: i32) {
            description("bad SQL (value_type_tag, value) pair")
            display("bad SQL (value_type_tag, value) pair: ({}, {:?})", value_type_tag, value.data_type())
        }

        // /// The SQLite store user_version isn't recognized.  This could be an old version of Mentat
        // /// trying to open a newer version SQLite store; or it could be a corrupt file; or ...
        // BadSQLiteStoreVersion(version: i32) {
        //     description("bad SQL store user_version")
        //     display("bad SQL store user_version: {}", version)
        // }

        /// A bootstrap definition couldn't be parsed or installed.  This is a programmer error, not
        /// a runtime error.
        BadBootstrapDefinition(t: String) {
            description("bad bootstrap definition")
            display("bad bootstrap definition: {}", t)
        }

        /// A schema assertion couldn't be parsed.
        BadSchemaAssertion(t: String) {
            description("bad schema assertion")
            display("bad schema assertion: {}", t)
        }

        /// An ident->entid mapping failed.
        UnrecognizedIdent(ident: String) {
            description("no entid found for ident")
            display("no entid found for ident: {}", ident)
        }

        /// An entid->ident mapping failed.
        /// We also use this error if you try to transact an entid that we didn't allocate,
        /// in part because we blow the stack in error_chain if we define a new enum!
        UnrecognizedEntid(entid: Entid) {
            description("unrecognized or no ident found for entid")
            display("unrecognized or no ident found for entid: {}", entid)
        }

        UnknownAttribute(attr: Entid) {
            description("unknown attribute")
            display("unknown attribute for entid: {}", attr)
        }

        CannotCacheNonUniqueAttributeInReverse(attr: Entid) {
            description("cannot reverse-cache non-unique attribute")
            display("cannot reverse-cache non-unique attribute: {}", attr)
        }

        SchemaAlterationFailed(t: String) {
            description("schema alteration failed")
            display("schema alteration failed: {}", t)
        }

        /// A transaction tried to violate a constraint of the schema of the Mentat store.
        SchemaConstraintViolation(violation: SchemaConstraintViolation) {
            description("schema constraint violation")
            display("schema constraint violation: {}", violation)
        }

        /// The transaction was malformed in some way (that was not recognized at parse time; for
        /// example, in a way that is schema-dependent).
        InputError(error: InputError) {
            description("transaction input error")
            display("transaction input error: {}", error)
        }
    }
}
