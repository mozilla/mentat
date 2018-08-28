// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use mentat_core::{
    Keyword,
};

use mentat_db::{
    CORE_SCHEMA_VERSION,
};

use public_traits::errors::{
    Result,
};

use tolstoy_traits::errors::{
    TolstoyError,
};

use datoms::{
    DatomsHelper,
};

use types::{
    Tx,
};

pub struct BootstrapHelper<'a> {
    parts: DatomsHelper<'a>
}

impl<'a> BootstrapHelper<'a> {
    pub fn new(assumed_bootstrap_tx: &Tx) -> BootstrapHelper {
        BootstrapHelper {
            parts: DatomsHelper::new(&assumed_bootstrap_tx.parts),
        }
    }

    // TODO we could also iterate through our own bootstrap schema definition and check that everything matches
    // "version" is used here as a proxy for doing that work
    pub fn is_compatible(&self) -> Result<bool> {
        Ok(self.core_schema_version()? == CORE_SCHEMA_VERSION as i64)
    }

    pub fn core_schema_version(&self) -> Result<i64> {
        match self.parts.ea_lookup(
            Keyword::namespaced("db.schema", "core"),
            Keyword::namespaced("db.schema", "version"),
        ) {
            Some(v) => {
                // TODO v is just a type tag and a Copy value, we shouldn't need to clone.
                match v.clone().into_long() {
                    Some(v) => Ok(v),
                    None => bail!(TolstoyError::BadRemoteState("incorrect type for core schema version".to_string()))
                }
            },
            None => bail!(TolstoyError::BadRemoteState("missing core schema version".to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use mentat_db::debug::{
        TestConn,
    };

    use debug::txs_after;

    #[test]
    fn test_bootstrap_version() {
        let remote = TestConn::default();

        let remote_txs = txs_after(&remote.sqlite, &remote.schema, remote.last_tx_id() - 1);

        assert_eq!(1, remote_txs.len());

        let bh = BootstrapHelper::new(&remote_txs[0]);
        assert_eq!(1, bh.core_schema_version().expect("schema version"));
    }
}
