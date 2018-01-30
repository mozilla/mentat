// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate mentat;

#[test]
fn can_import_sqlite() {
    // From https://github.com/jgallagher/rusqlite#rusqlite.
    #[derive(Debug)]
    struct Person {
        id: i32,
        name: String,
        data: Option<Vec<u8>>,
    }

    let conn = mentat::new_connection("").expect("SQLite connected");

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
