// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use mentat_query::{
    WhereFn,
};

use clauses::{
    ConjoiningClauses,
};

use errors::{
    AlgebrizerErrorKind,
    Result,
};

use Known;

/// Application of `where` functions.
impl ConjoiningClauses {
    /// There are several kinds of functions binding variables in our Datalog:
    /// - A set of functions like `ground`, fulltext` and `get-else` that are translated into SQL
    ///   `VALUES`, `MATCH`, or `JOIN`, yielding bindings.
    /// - In the future, some functions that are implemented via function calls in SQLite.
    ///
    /// At present we have implemented only a limited selection of functions.
    pub(crate) fn apply_where_fn(&mut self, known: Known, where_fn: WhereFn) -> Result<()> {
        // Because we'll be growing the set of built-in functions, handling each differently, and
        // ultimately allowing user-specified functions, we match on the function name first.
        match where_fn.operator.0.as_str() {
            "fulltext" => self.apply_fulltext(known, where_fn),
            "ground" => self.apply_ground(known, where_fn),
            "tx-data" => self.apply_tx_data(known, where_fn),
            "tx-ids" => self.apply_tx_ids(known, where_fn),
            _ => bail!(AlgebrizerErrorKind::UnknownFunction(where_fn.operator.clone())),
        }
    }
}
