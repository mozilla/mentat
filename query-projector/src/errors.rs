// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use rusqlite;

use mentat_core::{
    ValueTypeSet,
};

use mentat_db;

use mentat_query::{
    PlainSymbol,
};

use aggregates::{
    SimpleAggregationOp,
};

error_chain! {
    types {
        Error, ErrorKind, ResultExt, Result;
    }

    errors {
        /// We're just not done yet.  Message that the feature is recognized but not yet
        /// implemented.
        NotYetImplemented(t: String) {
            description("not yet implemented")
            display("not yet implemented: {}", t)
        }
        CannotProjectImpossibleBinding(op: SimpleAggregationOp) {
            description("no possible types for variable in projection list")
            display("no possible types for value provided to {:?}", op)
        }
        CannotApplyAggregateOperationToTypes(op: SimpleAggregationOp, types: ValueTypeSet) {
            description("cannot apply projection operation to types")
            display("cannot apply projection operation {:?} to types {:?}", op, types)
        }
        UnboundVariable(var: PlainSymbol) {
            description("cannot project unbound variable")
            display("cannot project unbound variable {:?}", var)
        }
        NoTypeAvailableForVariable(var: PlainSymbol) {
            description("cannot find type for variable")
            display("cannot find type for variable {:?}", var)
        }
        UnexpectedResultsType(actual: &'static str, expected: &'static str) {
            description("unexpected query results type")
            display("expected {}, got {}", expected, actual)
        }
        AmbiguousAggregates(min_max_count: usize, corresponding_count: usize) {
            description("ambiguous aggregates")
            display("min/max expressions: {} (max 1), corresponding: {}", min_max_count, corresponding_count)
        }
    }

    foreign_links {
        Rusqlite(rusqlite::Error);
    }

    links {
        DbError(mentat_db::Error, mentat_db::ErrorKind);
    }
}

