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

error_chain! {
    // The type defined for this error. These are the conventional
    // and recommended names, but they can be arbitrarily chosen.
    //
    // It is also possible to leave this section out entirely, or
    // leave it empty, and these names will be used automatically.
    types {
        Error, ErrorKind, ResultExt, Result;
    }

    // Without the `Result` wrapper:
    //
    // types {
    //     Error, ErrorKind, ResultExt;
    // }

    // // Automatic conversions between this error chain and other
    // // error chains. In this case, it will e.g. generate an
    // // `ErrorKind` variant called `Another` which in turn contains
    // // the `other_error::ErrorKind`, with conversions from
    // // `other_error::Error`.
    // //
    // // Optionally, some attributes can be added to a variant.
    // //
    // // This section can be empty.
    // links {
    //     Another(other_error::Error, other_error::ErrorKind) #[cfg(unix)];
    // }

    // Automatic conversions between this error chain and other
    // error types not defined by the `error_chain!`. These will be
    // wrapped in a new error with, in the first case, the
    // `ErrorKind::Fmt` variant. The description and cause will
    // forward to the description and cause of the original error.
    //
    // Optionally, some attributes can be added to a variant.
    //
    // This section can be empty.
    foreign_links {
        // Fmt(::std::fmt::Error);
        // Io(::std::io::Error) #[cfg(unix)];
        Rusqlite(rusqlite::Error);
    }

    // Define additional `ErrorKind` variants. The syntax here is
    // the same as `quick_error!`, but the `from()` and `cause()`
    // syntax is not supported.
    errors {
        /// Something went wrong at the SQLite level.
        RusqliteX { // (t: String) {
            description("SQLite error")
                // display("SQLite error: '{}'", t)
        }

        /// We're just not done yet.  Message that the feature is recognized but not yet
        /// implemented.
        NotYetImplemented(t: String) {
            description("not yet implemented")
            display("not yet implemented: {}", t)
        }

        /// We've got corrupt data in the SQL store: a value_type_tag isn't recognized!
        BadValueTypeTag(value_type_tag: i32) {
            description("bad value_type_tag")
            display("bad value_type_tag: {}", value_type_tag)
        }

        /// We've got corrupt data in the SQL store: a value and value_type_tag don't line up.
        BadValueAndTagPair(value: rusqlite::types::Value, value_type_tag: i32) {
            description("bad (value, value_type_tag) pair")
            display("bad (value_type_tag, value) pair: ({}, {:?})", value_type_tag, value.data_type())
        }

        /// The SQLite store user_version isn't recognized.  This could be an old version of Mentat
        /// trying to open a newer version SQLite store; or it could be a corrupt file; or ...
        BadSQLiteStoreVersion(version: i32) {
            description("bad SQL store user_version")
            display("bad SQL store user_version: {}", version)
        }

        /// A bootstrap definition couldn't be parsed or installed.  This is a programmer error, not
        /// a runtime error.
        BadBootstrapDefinition(t: String) {
            description("bad bootstrap definition")
            display("bad bootstrap definition: '{}'", t)
        }

        /// A schema assertion couldn't be parsed.
        BadSchemaAssertion(t: String) {
            description("bad schema assertion")
            display("bad schema assertion: '{}'", t)
        }
    }
}
