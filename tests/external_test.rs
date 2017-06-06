// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate time;

extern crate mentat;
extern crate mentat_db;

use mentat::{new_connection, conn};

use std::io::prelude::*;
use std::fs::File;


#[test]
fn can_import_sqlite() {
    // From https://github.com/jgallagher/rusqlite#rusqlite.
    #[derive(Debug)]
    struct Person {
        id: i32,
        name: String,
        data: Option<Vec<u8>>,
    }

    let conn = mentat::get_connection();

    conn.execute("CREATE TABLE person (
                  id              INTEGER PRIMARY KEY,
                  name            TEXT NOT NULL,
                  data            BLOB
                  )",
                 &[])
        .unwrap();
    let me = Person {
        id: 1,
        name: "Steven".to_string(),
        data: None,
    };
    conn.execute("INSERT INTO person (name, data)
                  VALUES (?1, ?2)",
                 &[&me.name, &me.data])
        .unwrap();

    let mut stmt = conn.prepare("SELECT id, name, data FROM person").unwrap();
    let person_iter = stmt.query_map(&[], |row| {
            Person {
                id: row.get(0),
                name: row.get(1),
                data: row.get(2),
            }
        })
        .unwrap();

    for person in person_iter {
        let p = person.unwrap();
        assert_eq!(me.id, p.id);
        assert_eq!(me.data, p.data);
        assert_eq!(me.data, p.data);
    }
}

#[test]
fn test_big() {
    let mut sqlite = new_connection("").unwrap();
    let mut conn = conn::Conn::connect(&mut sqlite).unwrap();

    let mut schema_file = File::open("tests/music-schema.dtm").expect("Unable to open the file");
    let mut schema_contents = String::new();
    schema_file.read_to_string(&mut schema_contents).expect("Unable to read the file.  TODO: Please download them at <URL>");
    let schema_transaction = conn.transact(&mut sqlite, schema_contents.as_str()).unwrap();
    assert_eq!(schema_transaction.tx_id, 0x10000000 + 1);

    // If you pull down the full dataset, you can replace the path here with
    // tests/music-data.dtm.
    let mut data_file = File::open("tests/music-data-partial.dtm").expect("Unable to open the file");
    let mut data_contents = String::new();
    data_file.read_to_string(&mut data_contents).expect("Unable to read the file.  TODO: Please download them at <URL>");
    let data_transaction = conn.transact(&mut sqlite, data_contents.as_str()).unwrap();
    assert_eq!(data_transaction.tx_id, 0x10000000 + 2);

    let results = conn.q_once(&mut sqlite,
        r#"[:find ?name
        :where
        [?p :artist/name ?name]]"#, None)
    .expect("Query failed");
    println!("{:?}", results);
    assert_eq!(874, results.len());
}
