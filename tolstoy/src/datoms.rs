// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use edn::{
    Keyword,
};

use core_traits::{
    Entid,
    TypedValue,
};

use types::TxPart;

/// A primitive query interface geared toward processing bootstrap-like sets of datoms.
pub struct DatomsHelper<'a> {
    parts: &'a Vec<TxPart>,
}

impl<'a> DatomsHelper<'a> {
    pub fn new(parts: &'a Vec<TxPart>) -> DatomsHelper {
        DatomsHelper {
            parts: parts,
        }
    }

    // TODO these are obviously quite inefficient
    pub fn e_lookup(&self, e: Keyword) -> Option<Entid> {
        // This wraps Keyword (e) in ValueRc (aliased Arc), which is rather expensive.
        let kw_e = TypedValue::Keyword(e.into());

        for part in self.parts {
            if kw_e == part.v && part.added {
                return Some(part.e);
            }
        }

        None
    }

    pub fn ea_lookup(&self, e: Keyword, a: Keyword) -> Option<&TypedValue> {
        let e_e = self.e_lookup(e);
        let a_e = self.e_lookup(a);

        if e_e.is_none() || a_e.is_none() {
            return None;
        }

        let e_e = e_e.unwrap();
        let a_e = a_e.unwrap();

        for part in self.parts {
            if part.e == e_e && part.a == a_e && part.added {
                return Some(&part.v);
            }
        }

        None
    }
}
