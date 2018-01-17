// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#[macro_use]
extern crate lazy_static;

extern crate mentat;
extern crate mentat_core;
extern crate mentat_db;
extern crate rusqlite;

use mentat::vocabulary;

use mentat::vocabulary::{
    VersionedStore,
    VocabularyCheck,
    VocabularyOutcome,
};

use mentat::query::IntoResult;

use mentat_core::{
    HasSchema,
};

use mentat::{
    Conn,
    NamespacedKeyword,
    Queryable,
    TypedValue,
    ValueType,
};

use mentat::entity_builder::BuildTerms;

use mentat::errors::{
    Error,
    ErrorKind,
};

lazy_static! {
    static ref FOO_NAME: NamespacedKeyword = {
        NamespacedKeyword::new("foo", "name")
    };

    static ref FOO_MOMENT: NamespacedKeyword = {
        NamespacedKeyword::new("foo", "moment")
    };

    static ref FOO_VOCAB: vocabulary::Definition = {
        vocabulary::Definition {
            name: NamespacedKeyword::new("org.mozilla", "foo"),
            version: 1,
            attributes: vec![
                (FOO_NAME.clone(),
                vocabulary::AttributeBuilder::default()
                    .value_type(ValueType::String)
                    .multival(false)
                    .unique(vocabulary::attribute::Unique::Identity)
                    .build()),
                (FOO_MOMENT.clone(),
                vocabulary::AttributeBuilder::default()
                    .value_type(ValueType::Instant)
                    .multival(false)
                    .index(true)
                    .build()),
            ]
        }
    };
}

// Do some work with the appropriate level of paranoia for a shared system.
fn be_paranoid(conn: &mut Conn, sqlite: &mut rusqlite::Connection, name: TypedValue, moment: TypedValue) {
    let mut in_progress = conn.begin_transaction(sqlite).expect("begun successfully");
    assert!(in_progress.verify_core_schema().is_ok());
    assert!(in_progress.ensure_vocabulary(&FOO_VOCAB).is_ok());

    let a_moment = in_progress.attribute_for_ident(&FOO_MOMENT).expect("exists").1;
    let a_name = in_progress.attribute_for_ident(&FOO_NAME).expect("exists").1;

    let builder = in_progress.builder();
    let mut entity = builder.describe_tempid("s");
    entity.add(a_name, name).expect("added");
    entity.add(a_moment, moment).expect("added");
    assert!(entity.commit().is_ok());      // Discard the TxReport.
}

#[test]
fn test_real_world() {
    let mut sqlite = mentat_db::db::new_connection("").unwrap();
    let mut conn = Conn::connect(&mut sqlite).unwrap();

    let alice: TypedValue = TypedValue::typed_string("Alice");
    let barbara: TypedValue = TypedValue::typed_string("Barbara");
    let now: TypedValue = TypedValue::current_instant();

    be_paranoid(&mut conn, &mut sqlite, alice.clone(), now.clone());
    be_paranoid(&mut conn, &mut sqlite, barbara.clone(), now.clone());

    let results = conn.q_once(&mut sqlite, r#"[:find ?name ?when
                                               :order (asc ?name)
                                               :where [?x :foo/name ?name]
                                                      [?x :foo/moment ?when]
                                               ]"#,
                              None)
                      .into_rel_result()
                      .expect("query succeeded");
    assert_eq!(results,
               vec![vec![alice, now.clone()], vec![barbara, now.clone()]]);
}

#[test]
fn test_add_vocab() {
    let bar = vocabulary::AttributeBuilder::default()
                  .value_type(ValueType::Instant)
                  .multival(false)
                  .index(true)
                  .build();
    let baz = vocabulary::AttributeBuilder::default()
                  .value_type(ValueType::String)
                  .multival(true)
                  .fulltext(true)
                  .build();
    let bar_only = vec![
        (NamespacedKeyword::new("foo", "bar"), bar.clone()),
    ];
    let baz_only = vec![
        (NamespacedKeyword::new("foo", "baz"), baz.clone()),
    ];
    let bar_and_baz = vec![
        (NamespacedKeyword::new("foo", "bar"), bar.clone()),
        (NamespacedKeyword::new("foo", "baz"), baz.clone()),
    ];

    let foo_v1_a = vocabulary::Definition {
        name: NamespacedKeyword::new("org.mozilla", "foo"),
        version: 1,
        attributes: bar_only.clone(),
    };

    let foo_v1_b = vocabulary::Definition {
        name: NamespacedKeyword::new("org.mozilla", "foo"),
        version: 1,
        attributes: bar_and_baz.clone(),
    };

    let mut sqlite = mentat_db::db::new_connection("").unwrap();
    let mut conn = Conn::connect(&mut sqlite).unwrap();

    let foo_version_query = r#"[:find [?version ?aa]
                                :where
                                [:org.mozilla/foo :db.schema/version ?version]
                                [:org.mozilla/foo :db.schema/attribute ?a]
                                [?a :db/ident ?aa]]"#;
    let foo_attributes_query = r#"[:find [?aa ...]
                                   :where
                                   [:org.mozilla/foo :db.schema/attribute ?a]
                                   [?a :db/ident ?aa]]"#;

    // Scoped borrow of `conn`.
    {
        let mut in_progress = conn.begin_transaction(&mut sqlite).expect("begun successfully");

        assert!(in_progress.verify_core_schema().is_ok());
        assert_eq!(VocabularyCheck::NotPresent, in_progress.check_vocabulary(&foo_v1_a).expect("check completed"));
        assert_eq!(VocabularyCheck::NotPresent, in_progress.check_vocabulary(&foo_v1_b).expect("check completed"));

        // If we install v1.a, then it will succeed.
        assert_eq!(VocabularyOutcome::Installed, in_progress.ensure_vocabulary(&foo_v1_a).expect("ensure succeeded"));

        // Now we can query to get the vocab.
        let ver_attr =
            in_progress.q_once(foo_version_query, None)
                       .into_tuple_result()
                       .expect("query returns")
                       .expect("a result");
        assert_eq!(ver_attr[0], TypedValue::Long(1));
        assert_eq!(ver_attr[1], TypedValue::typed_ns_keyword("foo", "bar"));

        // If we commit, it'll stick around.
        in_progress.commit().expect("commit succeeded");
    }

    // It's still there.
    let ver_attr =
        conn.q_once(&mut sqlite,
                    foo_version_query,
                    None)
            .into_tuple_result()
            .expect("query returns")
            .expect("a result");
    assert_eq!(ver_attr[0], TypedValue::Long(1));
    assert_eq!(ver_attr[1], TypedValue::typed_ns_keyword("foo", "bar"));

    // Scoped borrow of `conn`.
    {
        let mut in_progress = conn.begin_transaction(&mut sqlite).expect("begun successfully");

        // Subsequently ensuring v1.a again will succeed with no work done.
        assert_eq!(VocabularyCheck::Present, in_progress.check_vocabulary(&foo_v1_a).expect("check completed"));

        // Checking for v1.b will say that we have work to do.
        assert_eq!(VocabularyCheck::PresentButMissingAttributes {
            attributes: vec![&baz_only[0]],
        }, in_progress.check_vocabulary(&foo_v1_b).expect("check completed"));

        // Ensuring v1.b will succeed.
        assert_eq!(VocabularyOutcome::InstalledMissingAttributes,
                   in_progress.ensure_vocabulary(&foo_v1_b).expect("ensure succeeded"));

        // Checking v1.a or v1.b again will still succeed with no work done.
        assert_eq!(VocabularyCheck::Present, in_progress.check_vocabulary(&foo_v1_a).expect("check completed"));
        assert_eq!(VocabularyCheck::Present, in_progress.check_vocabulary(&foo_v1_b).expect("check completed"));

        // Ensuring again does nothing.
        assert_eq!(VocabularyOutcome::Existed, in_progress.ensure_vocabulary(&foo_v1_b).expect("ensure succeeded"));
        in_progress.commit().expect("commit succeeded");
    }

    // We have both attributes.
    let actual_attributes =
        conn.q_once(&mut sqlite,
                    foo_attributes_query,
                    None)
            .into_coll_result()
            .expect("query returns");
    assert_eq!(actual_attributes,
               vec![
                   TypedValue::typed_ns_keyword("foo", "bar"),
                   TypedValue::typed_ns_keyword("foo", "baz"),
               ]);

    // Now let's modify our vocabulary without bumping the version. This is invalid and will result
    // in an error.
    let malformed_baz = vocabulary::AttributeBuilder::default()
                            .value_type(ValueType::Instant)
                            .multival(true)
                            .build();
    let bar_and_malformed_baz = vec![
        (NamespacedKeyword::new("foo", "bar"), bar),
        (NamespacedKeyword::new("foo", "baz"), malformed_baz.clone()),
    ];
    let foo_v1_malformed = vocabulary::Definition {
        name: NamespacedKeyword::new("org.mozilla", "foo"),
        version: 1,
        attributes: bar_and_malformed_baz.clone(),
    };

    // Scoped borrow of `conn`.
    {
        let mut in_progress = conn.begin_transaction(&mut sqlite).expect("begun successfully");
        match in_progress.ensure_vocabulary(&foo_v1_malformed) {
            Result::Err(Error(ErrorKind::ConflictingAttributeDefinitions(vocab, version, attr, theirs, ours), _)) => {
                assert_eq!(vocab.as_str(), ":org.mozilla/foo");
                assert_eq!(attr.as_str(), ":foo/baz");
                assert_eq!(version, 1);
                assert_eq!(&theirs, &baz);
                assert_eq!(&ours, &malformed_baz);
            },
            _ => panic!(),
        }
    }
}
