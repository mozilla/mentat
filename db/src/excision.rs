// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::{
    BTreeSet,
    BTreeMap,
};

use mentat_core::{
    Attribute,
    Entid,
    HasSchema,
    Schema,
};

use entids;

use errors::{
    DbErrorKind,
    Result,
};

use internal_types::{
    AEVTrie,
    filter_aev_to_eav,
};

use schema::{
    SchemaBuilding,
};

use types::{
    PartitionMap,
};

/// Details about an excision:
/// - a target to excise (for now, an entid);
/// - a possibly empty set of attributes to excise (the empty set means all attributes, not no
///   attributes);
/// - and a possibly omitted transaction ID to limit the excision before.  (TODO: check whether
///   Datomic excises the last retraction before the first remaining assertion, and make our
///   behaviour agree.)
///
/// `:db/before` doesn't make sense globally, since in Mentat, monotonically increasing
/// transaction IDs don't guarantee monotonically increasing txInstant values.  Therefore, we
/// accept only `:db/beforeT` and allow consumers to turn `:db/before` timestamps into
/// transaction IDs in whatever way they see fit.
#[derive(Clone, Debug, Default)]
pub(crate) struct Excision {
    pub(crate) target: Entid,
    pub(crate) attrs: Option<BTreeSet<Entid>>,
    pub(crate) before_tx: Option<Entid>,
}

pub(crate) type Excisions = BTreeMap<Entid, Excision>;

/// Extract excisions from the given transacted datoms.
pub(crate) fn excisions<'schema>(partition_map: &'schema PartitionMap, schema: &'schema Schema, aev_trie: &AEVTrie<'schema>) -> Result<Option<Excisions>> {
    let pair = |a: Entid| -> Result<(Entid, &'schema Attribute)> {
        schema.require_attribute_for_entid(a).map(|attribute| (a, attribute))
    };

    if aev_trie.contains_key(&pair(entids::DB_EXCISE_BEFORE)?) {
        bail!(DbErrorKind::BadExcision(":db.excise/before".into())); // TODO: more details.
    }

    let eav_trie = filter_aev_to_eav(aev_trie, |&(a, _)|
                                     a == entids::DB_EXCISE ||
                                     a == entids::DB_EXCISE_ATTRS ||
                                     a == entids::DB_EXCISE_BEFORE_T);

    let mut excisions = BTreeMap::default();

    for (&e, avs) in eav_trie.iter() {
        for (&(_a, _attribute), ars) in avs {
            if !ars.retract.is_empty() {
                bail!(DbErrorKind::BadExcision("retraction".into())); // TODO: more details.
            }
        }

        let target = avs.get(&pair(entids::DB_EXCISE)?)
            .and_then(|ars| ars.add.iter().next().cloned())
            .and_then(|v| v.into_entid())
            .ok_or_else(|| DbErrorKind::BadExcision("no :db/excise".into()))?; // TODO: more details.

        if schema.get_ident(target).is_some() {
            bail!(DbErrorKind::BadExcision("cannot mutate schema".into())); // TODO: more details.
        }

        let partition = partition_map.partition_for_entid(target)
            .ok_or_else(|| DbErrorKind::BadExcision("target has no partition".into()))?; // TODO: more details.
        // Right now, Mentat only supports `:db.part/{db,user,tx}`, and tests hack in `:db.part/fake`.
        if partition == ":db.part/db" || partition == ":db.part/tx" {
            bail!(DbErrorKind::BadExcision(format!("cannot target entity in partition {}", partition).into())); // TODO: more details.
        }

        let before_tx = avs.get(&pair(entids::DB_EXCISE_BEFORE_T)?)
            .and_then(|ars| ars.add.iter().next().cloned())
            .and_then(|v| v.into_entid());

        let attrs = avs.get(&pair(entids::DB_EXCISE_ATTRS)?)
            .map(|ars| ars.add.clone().into_iter().filter_map(|v| v.into_entid()).collect());

        let excision = Excision {
            target,
            attrs,
            before_tx,
        };

        excisions.insert(e, excision);
    }

    if excisions.is_empty() {
        Ok(None)
    } else {
        Ok(Some(excisions))
    }
}
