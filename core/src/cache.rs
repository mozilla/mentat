// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

/// Cache traits.

use std::collections::{
    BTreeSet,
};

use ::{
    Entid,
    Schema,
    TypedValue,
};

pub trait CachedAttributes {
    fn is_attribute_cached_reverse(&self, entid: Entid) -> bool;
    fn is_attribute_cached_forward(&self, entid: Entid) -> bool;
    fn get_values_for_entid(&self, schema: &Schema, attribute: Entid, entid: Entid) -> Option<&Vec<TypedValue>>;
    fn get_value_for_entid(&self, schema: &Schema, attribute: Entid, entid: Entid) -> Option<&TypedValue>;

    /// Reverse lookup.
    fn get_entid_for_value(&self, attribute: Entid, value: &TypedValue) -> Option<Entid>;
    fn get_entids_for_value(&self, attribute: Entid, value: &TypedValue) -> Option<&BTreeSet<Entid>>;
}
