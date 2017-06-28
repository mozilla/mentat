// Copyright 2017 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use rusqlite;

use edn;

use errors as cli;

use mentat::{
    new_connection,
};

use mentat::query::QueryResults;

use mentat::conn::Conn;
use mentat_db::types::TxReport;

pub struct Store {
    handle: rusqlite::Connection,
    conn: Conn,
    pub db_name: String,
}

pub fn db_output_name(db_name: &String) -> String {
    if db_name.is_empty() { "in-memory db".to_string() } else { db_name.clone() }
}

impl Store {
    pub fn new(database: Option<String>) -> Result<Store, cli::Error> {
        let db_name = database.unwrap_or("".to_string());
        
        let mut handle = try!(new_connection(&db_name));        
        let conn = try!(Conn::connect(&mut handle)); 
        Ok(Store { handle, conn, db_name })
    }

    pub fn open(&mut self, database: Option<String>) -> Result<(), cli::Error> {
        self.db_name = database.unwrap_or("".to_string());
        self.handle = try!(new_connection(&self.db_name));
        self.conn = try!(Conn::connect(&mut self.handle));
        Ok(())
    }

    pub fn close(&mut self) -> Result<(), cli::Error> {
        self.db_name = "".to_string();
        self.open(None)
    }

    pub fn query(&self, query: String) -> Result<QueryResults, cli::Error> {
        Ok(self.conn.q_once(&self.handle, &query, None)?)
    }

    pub fn transact(&mut self, transaction: String) -> Result<TxReport, cli::Error> {
        Ok(self.conn.transact(&mut self.handle, &transaction)?)
    }

    // the schema is the entire schema of the store including structure used to describe the store.
    pub fn fetch_schema(&self) -> edn::Value {
        self.conn.current_schema().to_edn_value()
    }

    // the attributes are the specific attributes added to the schema for this particular store.
    pub fn fetch_attributes(&self) -> edn::Value {
        let schema = self.conn.current_schema();

        edn::Value::Vector((&schema.schema_map).iter()
            .filter_map(|(entid, attribute)| {
                if let Some(ident) = schema.get_ident(*entid) {
                    if !ident.namespace.starts_with("db") {
                        return Some(attribute.to_edn_value(Some(ident.clone())));
                    }
                }
                return None;
            })
            .collect())
    }
}
