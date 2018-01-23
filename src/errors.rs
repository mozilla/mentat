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

use rusqlite;

use std::collections::BTreeSet;

use edn;
use mentat_core::{
    Attribute,
};
use mentat_db;
use mentat_query;
use mentat_query_algebrizer;
use mentat_query_parser;
use mentat_query_projector;
use mentat_query_translator;
use mentat_sql;
use mentat_tx_parser;

error_chain! {
    types {
        Error, ErrorKind, ResultExt, Result;
    }

    foreign_links {
        EdnParseError(edn::ParseError);
        Rusqlite(rusqlite::Error);
    }

    links {
        DbError(mentat_db::Error, mentat_db::ErrorKind);
        QueryError(mentat_query_algebrizer::Error, mentat_query_algebrizer::ErrorKind);   // Let's not leak the term 'algebrizer'.
        QueryParseError(mentat_query_parser::Error, mentat_query_parser::ErrorKind);
        ProjectorError(mentat_query_projector::Error, mentat_query_projector::ErrorKind);
        TranslatorError(mentat_query_translator::Error, mentat_query_translator::ErrorKind);
        SqlError(mentat_sql::Error, mentat_sql::ErrorKind);
        TxParseError(mentat_tx_parser::Error, mentat_tx_parser::ErrorKind);
    }

    errors {
        UnboundVariables(names: BTreeSet<String>) {
            description("unbound variables at query execution time")
            display("variables {:?} unbound at query execution time", names)
        }

        InvalidArgumentName(name: String) {
            description("invalid argument name")
            display("invalid argument name: '{}'", name)
        }

        UnknownAttribute(name: String) {
            description("unknown attribute")
            display("unknown attribute: '{}'", name)
        }

        InvalidVocabularyVersion {
            description("invalid vocabulary version")
            display("invalid vocabulary version")
        }

        ConflictingAttributeDefinitions(vocabulary: String, version: ::vocabulary::Version, attribute: String, current: Attribute, requested: Attribute) {
            description("conflicting attribute definitions")
            display("vocabulary {}/{} already has attribute {}, and the requested definition differs", vocabulary, version, attribute)
        }
    }
}
