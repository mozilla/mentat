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
use mentat::{
    new_connection,
};

use mentat::conn::Conn;

pub struct Store {
    handle: rusqlite::Connection,
    conn: Conn,
}

fn db_output_name(db_name: &String) -> String {
    if db_name.is_empty() { "in memory db".to_string() } else { db_name.clone() }
}

impl Store {
    pub fn new(database: Option<String>) -> Store {
        let db_name = database.unwrap_or("".to_string());
        let output_name = db_output_name(&db_name);
        let mut handle = new_connection(db_name).expect("Couldn't open conn.");
        let conn = Conn::connect(&mut handle).expect("Couldn't open DB.");
        println!("Database {:?} opened", output_name);
        Store { handle, conn }
    }

    pub fn open(&mut self, database: Option<String>) {
        let db_name = database.unwrap_or("".to_string());
        let output_name = db_output_name(&db_name);
        self.handle = new_connection(db_name).expect("Couldn't open conn.");
        self.conn = Conn::connect(&mut self.handle).expect("Couldn't open DB.");
        println!("Database {:?} opened", output_name);
    }

}
