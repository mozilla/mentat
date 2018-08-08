// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate edn;

use edn::entities::{
    AttributePlace,
    EntityPlace,
    EntidOrIdent,
    ValuePlace,
    TransactableValueMarker,
};

/// Represents one entid in the entid space.
///
/// Per https://www.sqlite.org/datatype3.html (see also http://stackoverflow.com/a/8499544), SQLite
/// stores signed integers up to 64 bits in size.  Since u32 is not appropriate for our use case, we
/// use i64 rather than manually truncating u64 to u63 and casting to i64 throughout the codebase.
pub type Entid = i64;

/// An entid that's either already in the store, or newly allocated to a tempid.
/// TODO: we'd like to link this in some way to the lifetime of a particular PartitionMap.
#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct KnownEntid(pub Entid);

impl From<KnownEntid> for Entid {
    fn from(k: KnownEntid) -> Entid {
        k.0
    }
}

impl<V: TransactableValueMarker> Into<EntityPlace<V>> for KnownEntid {
    fn into(self) -> EntityPlace<V> {
        EntityPlace::Entid(EntidOrIdent::Entid(self.0))
    }
}

impl Into<AttributePlace> for KnownEntid {
    fn into(self) -> AttributePlace {
        AttributePlace::Entid(EntidOrIdent::Entid(self.0))
    }
}

impl<V: TransactableValueMarker> Into<ValuePlace<V>> for KnownEntid {
    fn into(self) -> ValuePlace<V> {
        ValuePlace::Entid(EntidOrIdent::Entid(self.0))
    }
}
