// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate edn;

use edn::symbols::Keyword;

pub type EntId = u32;            // TODO: u64? Not all DB values will be representable in a u32.

/// The ability to transform entity identifiers (entids) into keyword names (idents).
pub trait ToIdent {
    fn ident(&self, entid: EntId) -> Option<Keyword>;
}

/// The ability to transform idents into the corresponding entid.
pub trait ToEntId {
    fn entid(&self, ident: &Keyword) -> Option<EntId>;
}
