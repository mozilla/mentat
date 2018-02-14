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

use edn;
use rusqlite;

use mentat_tx_parser;
use types::{Entid, ValueType};

error_chain! {
    types {
        Error, ErrorKind, ResultExt, Result;
    }

    foreign_links {
        Rusqlite(rusqlite::Error);
    }

    links {
        TxParseError(mentat_tx_parser::Error, mentat_tx_parser::ErrorKind);
    }

    errors {
        /// We're just not done yet.  Message that the feature is recognized but not yet
        /// implemented.
        NotYetImplemented(t: String) {
            description("not yet implemented")
            display("not yet implemented: {}", t)
        }

        /// We've been given an EDN value that isn't the correct Mentat type.
        BadEDNValuePair(value: edn::types::Value, value_type: ValueType) {
            description("EDN value is not the expected Mentat value type")
            display("EDN value '{}' is not the expected Mentat value type {:?}", value, value_type)
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

        ConflictingDatoms {
            description("conflicting datoms in tx")
            display("conflicting datoms in tx")
        }

        UnknownAttribute(attr: Entid) {
            description("unknown attribute")
            display("unknown attribute for entid: {}", attr)
        }

        CannotCacheNonUniqueAttributeInReverse(attr: Entid) {
            description("cannot reverse-cache non-unique attribute")
            display("cannot reverse-cache non-unique attribute: {}", attr)
        }
    }
}
