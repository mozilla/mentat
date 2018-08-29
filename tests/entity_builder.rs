// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#[macro_use]
extern crate mentat;
extern crate mentat_core;
extern crate mentat_db;
extern crate mentat_transaction;
extern crate public_traits;
extern crate core_traits;
extern crate db_traits;

use mentat::conn::{
    Conn,
};

use core_traits::{
    Entid,
    KnownEntid,
    TypedValue,
};

use mentat_core::{
    HasSchema,
    TxReport,
};

use mentat_transaction::{
    TermBuilder,
    Queryable,
};

use public_traits::errors::{
    MentatError,
};

use mentat::entity_builder::{
    BuildTerms,
};

// In reality we expect the store to hand these out safely.
fn fake_known_entid(e: Entid) -> KnownEntid {
    KnownEntid(e)
}

#[test]
fn test_entity_builder_bogus_entids() {
    let mut builder = TermBuilder::new();
    let e = builder.named_tempid("x");
    let a1 = fake_known_entid(37);    // :db/doc
    let a2 = fake_known_entid(999);
    let v = TypedValue::typed_string("Some attribute");
    let ve = fake_known_entid(12345);

    builder.add(e.clone(), a1, v).expect("add succeeded");
    builder.add(e.clone(), a2, e.clone()).expect("add succeeded, even though it's meaningless");
    builder.add(e.clone(), a2, ve).expect("add succeeded, even though it's meaningless");
    let (terms, tempids) = builder.build().expect("build succeeded");

    assert_eq!(tempids.len(), 1);
    assert_eq!(terms.len(), 3);     // TODO: check the contents?

    // Now try to add them to a real store.
    let mut sqlite = mentat_db::db::new_connection("").unwrap();
    let conn = Conn::connect(&mut sqlite).unwrap();
    let mut in_progress = conn.begin_transaction(&mut sqlite).expect("begun successfully");

    // This should fail: unrecognized entid.
    match in_progress.transact_entities(terms).expect_err("expected transact to fail") {
        MentatError::DbError(e) => {
            assert_eq!(e.kind(), db_traits::errors::DbErrorKind::UnrecognizedEntid(999));
        },
        _ => panic!("Should have rejected the entid."),
    }
}

#[test]
fn test_in_progress_builder() {
    let mut sqlite = mentat_db::db::new_connection("").unwrap();
    let conn = Conn::connect(&mut sqlite).unwrap();

    // Give ourselves a schema to work with!
    conn.transact(&mut sqlite, r#"[
        [:db/add "o" :db/ident :foo/one]
        [:db/add "o" :db/valueType :db.type/long]
        [:db/add "o" :db/cardinality :db.cardinality/one]
        [:db/add "m" :db/ident :foo/many]
        [:db/add "m" :db/valueType :db.type/string]
        [:db/add "m" :db/cardinality :db.cardinality/many]
        [:db/add "r" :db/ident :foo/ref]
        [:db/add "r" :db/valueType :db.type/ref]
        [:db/add "r" :db/cardinality :db.cardinality/one]
    ]"#).unwrap();

    let in_progress = conn.begin_transaction(&mut sqlite).expect("begun successfully");

    // We can use this or not!
    let a_many = in_progress.get_entid(&kw!(:foo/many)).expect(":foo/many");

    let mut builder = in_progress.builder();
    let e_x = builder.named_tempid("x");
    let v_many_1 = TypedValue::typed_string("Some text");
    let v_many_2 = TypedValue::typed_string("Other text");
    builder.add(e_x.clone(), kw!(:foo/many), v_many_1).expect("add succeeded");
    builder.add(e_x.clone(), a_many, v_many_2).expect("add succeeded");
    builder.commit().expect("commit succeeded");
}

#[test]
fn test_entity_builder() {
    let mut sqlite = mentat_db::db::new_connection("").unwrap();
    let conn = Conn::connect(&mut sqlite).unwrap();

    let foo_one = kw!(:foo/one);
    let foo_many = kw!(:foo/many);
    let foo_ref = kw!(:foo/ref);
    let report: TxReport;

    // Give ourselves a schema to work with!
    // Scoped borrow of conn.
    {
        conn.transact(&mut sqlite, r#"[
            [:db/add "o" :db/ident :foo/one]
            [:db/add "o" :db/valueType :db.type/long]
            [:db/add "o" :db/cardinality :db.cardinality/one]
            [:db/add "m" :db/ident :foo/many]
            [:db/add "m" :db/valueType :db.type/string]
            [:db/add "m" :db/cardinality :db.cardinality/many]
            [:db/add "r" :db/ident :foo/ref]
            [:db/add "r" :db/valueType :db.type/ref]
            [:db/add "r" :db/cardinality :db.cardinality/one]
        ]"#).unwrap();

        let mut in_progress = conn.begin_transaction(&mut sqlite).expect("begun successfully");

        // Scoped borrow of in_progress.
        {
            let mut builder = TermBuilder::new();
            let e_x = builder.named_tempid("x");
            let e_y = builder.named_tempid("y");
            let a_ref = in_progress.get_entid(&foo_ref).expect(":foo/ref");
            let a_one = in_progress.get_entid(&foo_one).expect(":foo/one");
            let a_many = in_progress.get_entid(&foo_many).expect(":foo/many");
            let v_many_1 = TypedValue::typed_string("Some text");
            let v_many_2 = TypedValue::typed_string("Other text");
            let v_long: TypedValue = 123.into();

            builder.add(e_x.clone(), a_many, v_many_1).expect("add succeeded");
            builder.add(e_x.clone(), a_many, v_many_2).expect("add succeeded");
            builder.add(e_y.clone(), a_ref, e_x.clone()).expect("add succeeded");
            builder.add(e_x.clone(), a_one, v_long).expect("add succeeded");

            let (terms, tempids) = builder.build().expect("build succeeded");

            assert_eq!(tempids.len(), 2);
            assert_eq!(terms.len(), 4);

            report = in_progress.transact_entities(terms).expect("add succeeded");
            let x = report.tempids.get("x").expect("our tempid has an ID");
            let y = report.tempids.get("y").expect("our tempid has an ID");
            assert_eq!(in_progress.lookup_value_for_attribute(*y, &foo_ref).expect("lookup succeeded"),
                        Some(TypedValue::Ref(*x)));
            assert_eq!(in_progress.lookup_value_for_attribute(*x, &foo_one).expect("lookup succeeded"),
                        Some(TypedValue::Long(123)));
        }

        in_progress.commit().expect("commit succeeded");
    }

    // It's all still there after the commit.
    let x = report.tempids.get("x").expect("our tempid has an ID");
    let y = report.tempids.get("y").expect("our tempid has an ID");
    assert_eq!(conn.lookup_value_for_attribute(&mut sqlite, *y, &foo_ref).expect("lookup succeeded"),
                Some(TypedValue::Ref(*x)));
}
