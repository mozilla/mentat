// Copyright 2017 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use edn;

use errors::Result;

use mentat::{
    QueryExplanation,
    QueryOutput,
    Queryable,
    Store,
};

use mentat_db::types::TxReport;

pub struct OpenStore {
    store: Store,
    pub db_name: String,
}

pub fn db_output_name(db_name: &String) -> String {
    if db_name.is_empty() { "in-memory db".to_string() } else { db_name.clone() }
}

impl OpenStore {
    pub fn new(database: Option<String>) -> Result<OpenStore> {
        let db_name = database.unwrap_or("".to_string());
        Ok(OpenStore {
            store: Store::open(db_name.as_str())?,
            db_name: db_name,
        })
    }

    pub fn open(&mut self, database: Option<String>) -> Result<()> {
        let name = database.unwrap_or("".to_string());
        if self.db_name.is_empty() || name != self.db_name {
            let next = Store::open(name.as_str())?;
            self.db_name = name;
            self.store = next;
        }

        Ok(())
    }

    pub fn close(&mut self) -> Result<()> {
        self.db_name = "".to_string();
        self.open(None)
    }

    pub fn query(&self, query: String) -> Result<QueryOutput> {
        Ok(self.store.q_once(&query, None)?)
    }

    pub fn explain_query(&self, query: String) -> Result<QueryExplanation> {
        Ok(self.store.q_explain(&query, None)?)
    }

    pub fn transact(&mut self, transaction: String) -> Result<TxReport> {
        let mut tx = self.store.begin_transaction()?;
        let report = tx.transact(&transaction)?;
        tx.commit()?;
        Ok(report)
    }

    pub fn fetch_schema(&self) -> edn::Value {
        self.store.conn().current_schema().to_edn_value()
    }
}
