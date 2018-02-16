// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]

use std;
use hyper;
use rusqlite;
use uuid;
use mentat_db;
use serde_cbor;
use serde_json;

error_chain! {
    types {
        Error, ErrorKind, ResultExt, Result;
    }

    foreign_links {
        IOError(std::io::Error);
        HttpError(hyper::Error);
        HyperUriError(hyper::error::UriError);
        SqlError(rusqlite::Error);
        UuidParseError(uuid::ParseError);
        Utf8Error(std::str::Utf8Error);
        JsonError(serde_json::Error);
        CborError(serde_cbor::error::Error);
    }

    links {
        DbError(mentat_db::Error, mentat_db::ErrorKind);
    }

    errors {
        TxIncorrectlyMapped(n: usize) {
            description("encountered more than one uuid mapping for tx")
            display("expected one, found {} uuid mappings for tx", n)
        }

        UnexpectedState(t: String) {
            description("encountered unexpected state")
            display("encountered unexpected state: {}", t)
        }

        NotYetImplemented(t: String) {
            description("not yet implemented")
            display("not yet implemented: {}", t)
        }

        DuplicateMetadata(k: String) {
            description("encountered more than one metadata value for key")
            display("encountered more than one metadata value for key: {}", k)
        }

        TxProcessorUnfinished {
            description("Tx processor couldn't finish")
            display("Tx processor couldn't finish")
        }

        BadServerResponse(s: String) {
            description("Received bad response from the server")
            display("Received bad response from the server: {}", s)
        }
    }
}
