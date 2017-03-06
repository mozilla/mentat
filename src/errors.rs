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

use edn;
use mentat_db;
use mentat_query_parser;
use mentat_query_projector;
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
        QueryParseError(mentat_query_parser::Error, mentat_query_parser::ErrorKind);
        ProjectorError(mentat_query_projector::Error, mentat_query_projector::ErrorKind);
        SqlError(mentat_sql::Error, mentat_sql::ErrorKind);
        TxParseError(mentat_tx_parser::Error, mentat_tx_parser::ErrorKind);
    }

    errors {
        InvalidArgumentName(name: String) {
            description("invalid argument name")
            display("invalid argument name: '{}'", name)
        }
    }
}
