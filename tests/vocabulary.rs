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

#[macro_use]
extern crate mentat;
extern crate mentat_core;
extern crate mentat_db;
extern crate rusqlite;

use mentat::vocabulary;

use mentat::vocabulary::{
    VersionedStore,
    VocabularyCheck,
    VocabularyOutcome,
    VocabularyProvider,
};

use mentat::query::IntoResult;

use mentat_core::{
    HasSchema,
};

// To check our working.
use mentat_db::AttributeValidation;

use mentat::{
    Conn,
    NamespacedKeyword,
    Queryable,
    Store,
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
        kw!(:foo/name)
    };

    static ref FOO_MOMENT: NamespacedKeyword = {
        kw!(:foo/moment)
    };

    static ref FOO_VOCAB: vocabulary::Definition = {
        vocabulary::Definition {
            name: kw!(:org.mozilla/foo),
            version: 1,
            attributes: vec![
                (FOO_NAME.clone(),
                vocabulary::AttributeBuilder::helpful()
                    .value_type(ValueType::String)
                    .multival(false)
                    .unique(vocabulary::attribute::Unique::Identity)
                    .build()),
                (FOO_MOMENT.clone(),
                vocabulary::AttributeBuilder::helpful()
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
fn test_default_attributebuilder_complains() {
    // ::new is helpful. ::default is not.
    assert!(vocabulary::AttributeBuilder::default()
                  .value_type(ValueType::String)
                  .multival(true)
                  .fulltext(true)
                  .build()
                  .validate(|| "Foo".to_string())
                  .is_err());

    assert!(vocabulary::AttributeBuilder::helpful()
                  .value_type(ValueType::String)
                  .multival(true)
                  .fulltext(true)
                  .build()
                  .validate(|| "Foo".to_string())
                  .is_ok());
}

#[test]
fn test_add_vocab() {
    let bar = vocabulary::AttributeBuilder::helpful()
                  .value_type(ValueType::Instant)
                  .multival(false)
                  .index(true)
                  .build();
    let baz = vocabulary::AttributeBuilder::helpful()
                  .value_type(ValueType::String)
                  .multival(true)
                  .fulltext(true)
                  .build();
    let bar_only = vec![
        (kw!(:foo/bar), bar.clone()),
    ];
    let baz_only = vec![
        (kw!(:foo/baz), baz.clone()),
    ];
    let bar_and_baz = vec![
        (kw!(:foo/bar), bar.clone()),
        (kw!(:foo/baz), baz.clone()),
    ];

    let foo_v1_a = vocabulary::Definition {
        name: kw!(:org.mozilla/foo),
        version: 1,
        attributes: bar_only.clone(),
    };

    let foo_v1_b = vocabulary::Definition {
        name: kw!(:org.mozilla/foo),
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
        (kw!(:foo/bar), bar),
        (kw!(:foo/baz), malformed_baz.clone()),
    ];
    let foo_v1_malformed = vocabulary::Definition {
        name: kw!(:org.mozilla/foo),
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

    // Some alterations -- cardinality/one to cardinality/many, unique to weaker unique or
    // no unique, unindexed to indexed -- can be applied automatically, so long as you
    // bump the version number.

    let multival_bar = vocabulary::AttributeBuilder::helpful()
                  .value_type(ValueType::Instant)
                  .multival(true)
                  .index(true)
                  .build();
    let multival_bar_and_baz = vec![
        (kw!(:foo/bar), multival_bar),
        (kw!(:foo/baz), baz.clone()),
    ];

    let altered_vocabulary = vocabulary::Definition {
        name: kw!(:org.mozilla/foo),
        version: 2,
        attributes: multival_bar_and_baz,
    };

    // foo/bar starts single-valued.
    assert_eq!(false, conn.current_schema().attribute_for_ident(&kw!(:foo/bar)).expect("attribute").0.multival);

    // Scoped borrow of `conn`.
    {
        let mut in_progress = conn.begin_transaction(&mut sqlite).expect("begun successfully");
        assert_eq!(in_progress.ensure_vocabulary(&altered_vocabulary).expect("success"),
                   VocabularyOutcome::Upgraded);
        in_progress.commit().expect("commit succeeded");
    }

    // Now it's multi-valued.
    assert_eq!(true, conn.current_schema().attribute_for_ident(&kw!(:foo/bar)).expect("attribute").0.multival);
}

// This is a real-world-style test that evolves a schema with data changes.
// We start with a basic vocabulary in three parts:
//
// Part 1 describes foods by name.
// Part 2 describes movies by title.
// Part 3 describes people: their names and heights, and their likes.
//
// We simulate three common migrations:
// - We made a trivial modeling error: movie names should not be unique.
// - We made a less trivial modeling error, one that can fail: food names should be unique so that
//   we can more easily refer to them during writes.
//   In order for this migration to succeed, we need to merge duplicates, then alter the schema --
//   which we will do by introducing a new property in the same vocabulary, deprecating the old one
//   -- then transact the transformed data.
// - We need to normalize some non-unique data: we recorded heights in inches when they should be
//   in centimeters.
// - We need to normalize some unique data: food names should all be lowercase. Again, that can fail
//   because of a uniqueness constraint. (We might know that it can't fail thanks to application
//   restrictions, in which case we can treat this as we did the height alteration.)
// - We made a more significant modeling error: we used 'like' to identify both movies and foods,
//   and we have decided that food preferences and movie preferences should be different attributes.
//   We wish to split these up and deprecate the old attribute. In order to do so we need to retract
//   all of the datoms that use the old attribute, transact new attributes _in both movies and foods_,
//   then re-assert the data.
#[test]
fn test_upgrade_with_functions() {
    let mut store = Store::open("").expect("open");

    let food_v1 = vocabulary::Definition {
        name: kw!(:org.mozilla/food),
        version: 1,
        attributes: vec![
            (kw!(:food/name),
             vocabulary::AttributeBuilder::helpful()
                .value_type(ValueType::String)
                .multival(false)
                .build()),
        ],
    };

    let movies_v1 = vocabulary::Definition {
        name: kw!(:org.mozilla/movies),
        version: 1,
        attributes: vec![
            (kw!(:movie/year),
             vocabulary::AttributeBuilder::helpful()
                .value_type(ValueType::Long)             // No need for Instant here.
                .multival(false)
                .build()),
            (kw!(:movie/title),
             vocabulary::AttributeBuilder::helpful()
                .value_type(ValueType::String)
                .multival(false)
                .unique(vocabulary::attribute::Unique::Identity)
                .index(true)
                .build()),
        ],
    };

    let people_v1 = vocabulary::Definition {
        name: kw!(:org.mozilla/people),
        version: 1,
        attributes: vec![
            (kw!(:person/name),
             vocabulary::AttributeBuilder::helpful()
                .value_type(ValueType::String)
                .multival(false)
                .unique(vocabulary::attribute::Unique::Identity)
                .index(true)
                .build()),
            (kw!(:person/height),
             vocabulary::AttributeBuilder::helpful()
                .value_type(ValueType::Long)
                .multival(false)
                .build()),
            (kw!(:person/likes),
             vocabulary::AttributeBuilder::helpful()
                .value_type(ValueType::Ref)
                .multival(true)
                .build()),
        ],
    };

    // Apply v1 of each.
    let v1_provider = VocabularyProvider {
        pre: |_ip| Ok(()),
        definitions: vec![
            food_v1.clone(),
            movies_v1.clone(),
            people_v1.clone(),
        ],
        post: |_ip| Ok(()),
    };

    // Mutable borrow of store.
    {
        let mut in_progress = store.begin_transaction().expect("began");

        in_progress.ensure_vocabularies(&v1_provider).expect("success");

        // Also add some data. We do this in one transaction 'cos -- thanks to the modeling errors
        // we are about to fix! -- it's a little awkward to make references to entities without
        // unique attributes.
        in_progress.transact(r#"[
            {:movie/title "John Wick"
             :movie/year 2014
             :db/id "mjw"}
            {:movie/title "Terminator 2: Judgment Day"
             :movie/year 1991
             :db/id "mt2"}
            {:movie/title "Dune"
             :db/id "md"
             :movie/year 1984}
            {:movie/title "Upstream Color"
             :movie/year 2013
             :db/id "muc"}
            {:movie/title "Primer"
             :db/id "mp"
             :movie/year 2004}

            ;; No year: not yet released.
            {:movie/title "The Modern Ocean"
             :db/id "mtmo"}

            {:food/name "Carrots" :db/id "fc"}
            {:food/name "Weird blue worms" :db/id "fwbw"}
            {:food/name "Spice" :db/id "fS"}
            {:food/name "spice" :db/id "fs"}

            ;; Sam likes action movies, carrots, and lowercase spice.
            {:person/name "Sam"
             :person/height 64
             :person/likes ["mjw", "mt2", "fc", "fs"]}

            ;; Beth likes thoughtful and weird movies, weird blue worms, and Spice.
            {:person/name "Beth"
             :person/height 68
             :person/likes ["muc", "mp", "md", "fwbw", "fS"]}

        ]"#).expect("transacted");

        in_progress.commit().expect("commit succeeded");
    }

    // Mutable borrow of store.
    {

        // Crap, there are several movies named Dune. We need to de-uniqify that attribute.
        let movies_v2 = vocabulary::Definition {
            name: kw!(:org.mozilla/movies),
            version: 2,
            attributes: vec![
                (kw!(:movie/title),
                 vocabulary::AttributeBuilder::helpful()
                    .value_type(ValueType::String)
                    .multival(false)
                    .non_unique()
                    .index(true)
                    .build()),
            ],
        };
        let mut in_progress = store.begin_transaction().expect("began");
        in_progress.ensure_vocabulary(&movies_v2).expect("success");

        // We can now add another Dune movie: Denis Villeneuve's 2019 version.
        // (Let's just pretend that it's been released, here in 2018!)
        in_progress.transact(r#"[
            {:movie/title "Dune"
             :movie/year 2019}
        ]"#).expect("transact succeeded");

        // And we can query both.
        let years =
            in_progress.q_once(r#"[:find [?year ...]
                                   :where [?movie :movie/title "Dune"]
                                           [?movie :movie/year ?year]
                                   :order (asc ?year)]"#, None)
                       .into_coll_result()
                       .expect("coll");
        assert_eq!(years, vec![TypedValue::Long(1984), TypedValue::Long(2019)]);
        in_progress.commit().expect("commit succeeded");
    }
}
