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
extern crate lazy_static;

#[macro_use]
extern crate mentat;
extern crate mentat_core;
extern crate mentat_db;
extern crate rusqlite;

use mentat::vocabulary;

use mentat::vocabulary::{
    Definition,
    SimpleVocabularySource,
    Version,
    VersionedStore,
    Vocabulary,
    VocabularyCheck,
    VocabularyOutcome,
    VocabularySource,
    VocabularyStatus,
};

use mentat::query::IntoResult;

use mentat_core::{
    HasSchema,
};

// To check our working.
use mentat_db::AttributeValidation;

use mentat::{
    Conn,
    InProgress,
    KnownEntid,
    NamespacedKeyword,
    QueryInputs,
    Queryable,
    RelResult,
    Store,
    Binding,
    TypedValue,
    ValueType,
};

use mentat::entity_builder::{
    BuildTerms,
    TermBuilder,
};

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
            ],
            pre: Definition::no_op,
            post: Definition::no_op,
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
               vec![vec![alice, now.clone()], vec![barbara, now.clone()]].into());
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

    let foo_v1_a = vocabulary::Definition::new(kw!(:org.mozilla/foo), 1, bar_only.clone());
    let foo_v1_b = vocabulary::Definition::new(kw!(:org.mozilla/foo), 1, bar_and_baz.clone());

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
        assert_eq!(ver_attr[0], TypedValue::Long(1).into());
        assert_eq!(ver_attr[1], TypedValue::typed_ns_keyword("foo", "bar").into());

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
    assert_eq!(ver_attr[0], TypedValue::Long(1).into());
    assert_eq!(ver_attr[1], TypedValue::typed_ns_keyword("foo", "bar").into());

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
                   TypedValue::typed_ns_keyword("foo", "bar").into(),
                   TypedValue::typed_ns_keyword("foo", "baz").into(),
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

    let foo_v1_malformed = vocabulary::Definition::new(
        kw!(:org.mozilla/foo),
        1,
        bar_and_malformed_baz.clone()
    );

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

    let altered_vocabulary = vocabulary::Definition::new(
        kw!(:org.mozilla/foo),
        2,
        multival_bar_and_baz
    );

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

/// A helper to turn rows from `[:find ?e ?a :where [?e ?a ?v]]` into a tuple.
fn ea(row: Vec<Binding>) -> (KnownEntid, KnownEntid) {
    let mut row = row.into_iter();
    match (row.next(), row.next()) {
        (Some(Binding::Scalar(TypedValue::Ref(e))), Some(Binding::Scalar(TypedValue::Ref(a)))) => {
            (KnownEntid(e), KnownEntid(a))
        },
        _ => panic!("Incorrect query shape for 'ea' helper."),
    }
}

/// A helper to turn rows from `[:find ?a ?v :where [?e ?a ?v]]` into a tuple.
/// Panics if any of the values are maps or vecs.
fn av(row: Vec<Binding>) -> (KnownEntid, TypedValue) {
    let mut row = row.into_iter();
    match (row.next(), row.next()) {
        (Some(Binding::Scalar(TypedValue::Ref(a))), Some(v)) => {
            (KnownEntid(a), v.val().unwrap())
        },
        _ => panic!("Incorrect query shape for 'av' helper."),
    }
}

/// A helper to turn rows from `[:find ?e ?v :where [?e ?a ?v]]` into a tuple.
/// Panics if any of the values are maps or vecs.
fn ev(row: Vec<Binding>) -> (KnownEntid, TypedValue) {
    // This happens to be the same as `av`.
    av(row)
}

type Inches = i64;
type Centimeters = i64;

/// ```
/// assert_eq!(inches_to_cm(100), 254);
/// ```
fn inches_to_cm(inches: Inches) -> Centimeters {
    (inches as f64 * 2.54f64) as Centimeters
}

fn height_of_person(in_progress: &InProgress, name: &str) -> Option<i64> {
    let h = in_progress.q_once(r#"[:find ?h .
                                    :in ?name
                                    :where [?p :person/name ?name]
                                            [?p :person/height ?h]]"#,
                                QueryInputs::with_value_sequence(vec![(var!(?name), TypedValue::typed_string(name))]))
                        .into_scalar_result()
                        .expect("result");
    match h {
        Some(Binding::Scalar(TypedValue::Long(v))) => Some(v),
        _ => None,
    }
}

// This is a real-world-style test that evolves a schema with data changes.
// We start with a basic vocabulary in three parts:
//
// Part 1 describes foods by name.
// Part 2 describes movies by title.
// Part 3 describes people: their names and heights, and their likes.
//
// We simulate four common migrations:
//
// 1. We made a trivial modeling error: movie names should not be unique.
//    We simply fix this -- removing a uniqueness constraint cannot fail.
//
// 2. We need to normalize some non-unique data: we recorded heights in inches when they should be
//    in centimeters. We fix this with a migration function and a version bump on the people schema.
//
// 3. We need to normalize some data: food names should all be lowercase, and they should be unique
//    so that we can more easily refer to them during writes.
//
//    That combination of changes can fail in either order if there are currently foods whose names
//    differ only by case.
//
//    (We might know that it can't fail thanks to application restrictions, in which case we can
//    treat this as we did the height alteration.)
//
//    In order for this migration to succeed, we need to merge duplicates, then alter the schema.
//
// 4. We made a more significant modeling error: we used 'like' to identify both movies and foods,
//    and we have decided that food preferences and movie preferences should be different attributes.
//    We wish to split these up and deprecate the old attribute. In order to do so we need to retract
//    all of the datoms that use the old attribute, transact new attributes _in both movies and foods_,
//    then re-assert the data.
//
//    This one's a little contrived, because it can also be solved without cross-vocabulary work,
//    but it's close enough to reality to be illustrative.
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
        pre: Definition::no_op,
        post: Definition::no_op,
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
        pre: Definition::no_op,
        post: Definition::no_op,
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
        pre: Definition::no_op,
        post: Definition::no_op,
    };

    // Apply v1 of each.
    let mut v1_provider = SimpleVocabularySource::with_definitions(
        vec![
            food_v1.clone(),
            movies_v1.clone(),
            people_v1.clone(),
        ]);

    // Mutable borrow of store.
    {
        let mut in_progress = store.begin_transaction().expect("began");

        in_progress.ensure_vocabularies(&mut v1_provider).expect("success");

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

    //
    // Migration 1: removal of a uniqueness constraint.
    //

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
        pre: Definition::no_op,
        post: Definition::no_op,
    };

    // Mutable borrow of store.
    {
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
        assert_eq!(years, vec![Binding::Scalar(TypedValue::Long(1984)), Binding::Scalar(TypedValue::Long(2019))]);
        in_progress.commit().expect("commit succeeded");
    }

    //
    // Migration 2: let's fix those heights!
    //

    fn convert_heights_to_centimeters(ip: &mut InProgress, from: &Vocabulary) -> mentat::errors::Result<()> {
        let mut builder = TermBuilder::new();

        // We keep a redundant safety check here to avoid running twice!
        if from.version < 2 {
            // Find every height and multiply it by 2.54.
            let person_height = ip.get_entid(&kw!(:person/height)).unwrap();
            for row in ip.q_once("[:find ?p ?h :where [?p :person/height ?h]]", None)
                         .into_rel_result()?
                         .into_iter() {
                let mut row = row.into_iter();
                match (row.next(), row.next()) {
                    (Some(Binding::Scalar(TypedValue::Ref(person))), Some(Binding::Scalar(TypedValue::Long(height)))) => {
                        // No need to explicitly retract: cardinality-one.
                        builder.add(KnownEntid(person), person_height, TypedValue::Long(inches_to_cm(height)))?;
                    },
                    _ => {},
                }
            }
        }

        if builder.is_empty() {
            return Ok(());
        }

        ip.transact_builder(builder).and(Ok(()))
    }

    fn people_v1_to_v2(ip: &mut InProgress, from: &Vocabulary) -> mentat::errors::Result<()> {
        convert_heights_to_centimeters(ip, from)?;

        // Let's update our documentation, too.
        if from.version < 2 {
            ip.transact(r#"[
                [:db/add :person/height :db/doc "A person's height in centimeters."]
            ]"#)?;
        }
        Ok(())
    }

    // Note that this definition is exactly the same as v1, but the version number is different,
    // and we add some functions to do cleanup.
    let people_v2 = vocabulary::Definition {
        name: kw!(:org.mozilla/people),
        version: 2,
        attributes: people_v1.attributes.clone(),
        pre: Definition::no_op,
        post: people_v1_to_v2,
    };

    // Mutable borrow of store.
    {
        let mut in_progress = store.begin_transaction().expect("began");

        // Before, Sam's height is 64 (inches).
        assert_eq!(Some(64), height_of_person(&in_progress, "Sam"));

        in_progress.ensure_vocabulary(&people_v2).expect("expected success");

        // Now, Sam's height is 162 (centimeters).
        assert_eq!(Some(162), height_of_person(&in_progress, "Sam"));

        in_progress.commit().expect("commit succeeded");
    }

    //
    // Migration 3: food names should be unique and lowercase.
    // Unfortunately, we have "spice" and "Spice"!
    //

    /// This is a straightforward migration -- replace the old name with the new one.
    /// This would be everything we need if we _knew_ there were no collisions (e.g., we were
    /// cleaning up UUIDs).
    fn lowercase_names(ip: &mut InProgress) -> mentat::errors::Result<()> {
        let food_name = ip.get_entid(&kw!(:food/name)).unwrap();
        let mut builder = TermBuilder::new();
        for row in ip.q_once("[:find ?f ?name :where [?f :food/name ?name]]", None)
                     .into_rel_result()?
                     .into_iter() {
            let mut row = row.into_iter();
            match (row.next(), row.next()) {
                (Some(Binding::Scalar(TypedValue::Ref(food))), Some(Binding::Scalar(TypedValue::String(name)))) => {
                    if name.chars().any(|c| !c.is_lowercase()) {
                        let lowercased = name.to_lowercase();
                        println!("Need to rename {} from '{}' to '{}'", food, name, lowercased);

                        let new_name: TypedValue = lowercased.into();
                        builder.add(KnownEntid(food), food_name, new_name)?;
                    }
                },
                _ => {},
            }
        }

        if builder.is_empty() {
            return Ok(());
        }

        ip.transact_builder(builder).and(Ok(()))
    }

    /// This is the function we write to dedupe. This logic is very suitable for sharing:
    /// indeed, "make this attribute unique by merging entities" is something we should
    /// lift out for reuse.
    fn merge_foods_with_same_name(ip: &mut InProgress) -> mentat::errors::Result<()> {
        let mut builder = TermBuilder::new();
        for row in ip.q_once("[:find ?a ?b
                               :where [?a :food/name ?name]
                                      [?b :food/name ?name]
                                      [(unpermute ?a ?b)]]", None)
                     .into_rel_result()?
                     .into_iter() {
            let mut row = row.into_iter();
            match (row.next(), row.next()) {
                (Some(Binding::Scalar(TypedValue::Ref(left))), Some(Binding::Scalar(TypedValue::Ref(right)))) => {
                    let keep = KnownEntid(left);
                    let replace = KnownEntid(right);

                    // For each use of the second entity, retract it and re-assert with the first.
                    // We should offer some support for doing this, 'cos this is long-winded and has
                    // the unexpected side-effect of also trying to retract metadata about the entity…
                    println!("Replacing uses of {} to {}.", replace.0, keep.0);
                    for (a, v) in ip.q_once("[:find ?a ?v
                                              :in ?old
                                              :where [?old ?a ?v]]",
                                            QueryInputs::with_value_sequence(vec![(var!(?old), replace.into())]))
                                    .into_rel_result()?
                                    .into_iter()
                                    .map(av) {
                        builder.retract(replace, a, v.clone())?;
                        builder.add(keep, a, v)?;
                    }
                    for (e, a) in ip.q_once("[:find ?e ?a
                                              :in ?old
                                              :where [?e ?a ?old]]",
                                            QueryInputs::with_value_sequence(vec![(var!(?old), replace.into())]))
                                    .into_rel_result()?
                                    .into_iter()
                                    .map(ea) {
                        builder.retract(e, a, replace)?;
                        builder.add(e, a, keep)?;
                    }

                    // TODO: `retractEntity` on `replace` (when we support that).
                },
                _ => {},
            }
        }

        if builder.is_empty() {
            return Ok(());
        }

        ip.transact_builder(builder).and(Ok(()))
    }

    // This migration is bad: it can't impose the uniqueness constraint because we end up with
    // two entities both with `:food/name "spice"`. We expect it to fail.
    let food_v2_bad = vocabulary::Definition {
        name: kw!(:org.mozilla/food),
        version: 2,
        attributes: vec![
            (kw!(:food/name),
             vocabulary::AttributeBuilder::helpful()
                .value_type(ValueType::String)
                .multival(false)
                .unique(vocabulary::attribute::Unique::Identity)
                .build()),
        ],
        pre: |ip, from| {
            if from.version < 2 {
                lowercase_names(ip)                    // <- no merging!
            } else {
                Ok(())
            }
        },
        post: Definition::no_op,
    };

    // This migration is better: once we rewrite the names, we merge the entities.
    let food_v2_good = vocabulary::Definition {
        name: kw!(:org.mozilla/food),
        version: 2,
        attributes: vec![
            (kw!(:food/name),
             vocabulary::AttributeBuilder::helpful()
                .value_type(ValueType::String)
                .multival(false)
                .unique(vocabulary::attribute::Unique::Identity)
                .build()),
        ],
        pre: |ip, from| {
            if from.version < 2 {
                lowercase_names(ip).and_then(|_| merge_foods_with_same_name(ip))
            } else {
                Ok(())
            }
        },
        post: Definition::no_op,
    };

    // Mutable borrow of store.
    {
        let mut in_progress = store.begin_transaction().expect("began");

        // Yep, the bad one fails!
        let _err = in_progress.ensure_vocabulary(&food_v2_bad).expect_err("expected error");
    }

    // Before the good migration, Sam and Beth don't like any of the same foods.
    assert!(store.q_once(r#"[:find [?food ...]
                             :where [?sam :person/name "Sam"]
                                    [?beth :person/name "Beth"]
                                    [?sam :person/likes ?f]
                                    [?beth :person/likes ?f]
                                    [?f :food/name ?food]]"#, None)
                 .into_coll_result()
                 .expect("success")
                 .is_empty());

    // Mutable borrow of store.
    {
        let mut in_progress = store.begin_transaction().expect("began");

        // The good one succeeded!
        in_progress.ensure_vocabulary(&food_v2_good).expect("expected success");
        in_progress.commit().expect("commit succeeded");
    }

    // After, Sam and Beth both like "spice" — the same entity.
    assert_eq!(store.q_once(r#"[:find [?food ...]
                                :where [?sam :person/name "Sam"]
                                       [?beth :person/name "Beth"]
                                       [?sam :person/likes ?f]
                                       [?beth :person/likes ?f]
                                       [?f :food/name ?food]]"#, None)
                    .into_coll_result()
                    .expect("success"),
               vec![TypedValue::typed_string("spice").into()]);

    //
    // Migration 4: multi-definition migration.
    //
    // Here we apply a function to a collection of definitions, not just the definitions
    // themselves.
    //
    // In this example we _could_ split up the work -- have :org.mozilla/movies port the movie
    // likes, and :org.mozilla/food port the food likes -- but for the sake of illustration we'll
    // write an enclosing function to do it.
    //
    // Further, the v3 versions of all vocabularies _can be applied to earlier versions_
    // and our migrations will still work. This vocabulary definition set will work for empty, v1, and
    // v2 of each definition.
    //
    let movies_v3 = vocabulary::Definition {
        name: kw!(:org.mozilla/movies),
        version: 3,
        attributes: vec![
            (kw!(:movie/title),
             vocabulary::AttributeBuilder::helpful()
                .value_type(ValueType::String)
                .multival(false)
                .non_unique()
                .index(true)
                .build()),

            // This phrasing is backward, but this is just a test.
            (kw!(:movie/likes),
             vocabulary::AttributeBuilder::helpful()
                .value_type(ValueType::Ref)
                .multival(true)
                .build()),
        ],
        pre: Definition::no_op,
        post: Definition::no_op,
    };
    let food_v3 = vocabulary::Definition {
        name: kw!(:org.mozilla/food),
        version: 3,
        attributes: vec![
            (kw!(:food/name),
             vocabulary::AttributeBuilder::helpful()
                .value_type(ValueType::String)
                .multival(false)
                .unique(vocabulary::attribute::Unique::Identity)
                .build()),

            // This phrasing is backward, but this is just a test.
            (kw!(:food/likes),
             vocabulary::AttributeBuilder::helpful()
                .value_type(ValueType::Ref)
                .multival(true)
                .build()),
        ],
        pre: |ip, from| {
            if from.version < 2 {
                lowercase_names(ip).and_then(|_| merge_foods_with_same_name(ip))?;
            }
            if from.version < 3 {
                // Nothing we need to do here. We're going to do the 'like' split outside.
            }
            Ok(())
        },
        post: Definition::no_op,
    };
    let people_v3 = vocabulary::Definition {
        name: kw!(:org.mozilla/people),
        version: 3,
        attributes: people_v1.attributes.clone(),
        pre: Definition::no_op,
        post: |ip, from| {
            if from.version < 2 {
                people_v1_to_v2(ip, from)?;
            }
            if from.version < 3 {
                // Nothing we need to do here. We're going to do the 'like' split outside.
            }
            Ok(())
        },
    };

    // For this more complex option, let's implement the VocabularySource trait rather than
    // using the simple struct. This will allow us to build stateful migrations where the
    // pre and post steps are linked.
    struct CustomVocabulary {
        pre_food_version: Version,
        post_done: bool,
        definitions: Vec<Definition>,
    }

    let mut all_three = CustomVocabulary {
        pre_food_version: 0,
        post_done: false,
        definitions: vec![people_v3, food_v3, movies_v3],
    };

    impl VocabularySource for CustomVocabulary {
        fn definitions(&mut self) -> Vec<Definition> {
            self.definitions.clone()
        }

        fn pre(&mut self, _in_progress: &mut InProgress, checks: &VocabularyStatus) -> mentat::errors::Result<()> {
            // Take a look at the work the vocabulary manager thinks needs to be done, and see whether we
            // need to migrate data.
            // We'll simulate that here by tracking the version.
            match checks.get(&kw!(:org.mozilla/food)) {
                Some((_, &VocabularyCheck::PresentButNeedsUpdate { ref older_version })) => {
                    self.pre_food_version = older_version.version;
                    Ok(())
                },
                _ => {
                    panic!("Test expected a different state.");
                },
            }
        }

        fn post(&mut self, ip: &mut InProgress) -> mentat::errors::Result<()> {
            self.post_done = true;

            // We know that if this function is running, some upgrade was necessary, and we're now
            // at v3 for all of the vocabulary we care about.
            // Given that this change is safe to try more than once, we don't bother checking versions here.
            let mut builder = TermBuilder::new();
            let person_likes = ip.get_entid(&kw!(:person/likes)).unwrap();
            let food_likes = ip.get_entid(&kw!(:food/likes)).unwrap();
            let movie_likes = ip.get_entid(&kw!(:movie/likes)).unwrap();
            let db_doc = ip.get_entid(&kw!(:db/doc)).unwrap();

            // Find all uses of :person/likes, splitting them into the two properties we just
            // introduced.
            for (e, v) in ip.q_once("[:find ?e ?v
                                      :where [?e :person/likes ?v]
                                             [?v :food/name _]]",
                                    None)
                            .into_rel_result()?
                            .into_iter()
                            .map(ev) {
                builder.retract(e, person_likes, v.clone())?;
                builder.add(e, food_likes, v)?;
            }

            for (e, v) in ip.q_once("[:find ?e ?v
                                      :where [?e :person/likes ?v]
                                             [?v :movie/title _]]",
                                    None)
                            .into_rel_result()?
                            .into_iter()
                            .map(ev) {
                builder.retract(e, person_likes, v.clone())?;
                builder.add(e, movie_likes, v)?;
            }

            builder.add(person_likes, db_doc,
                        TypedValue::typed_string("Deprecated. Use :movie/likes or :food/likes instead."))?;
            ip.transact_builder(builder).and(Ok(()))
        }
    };

    {
        let mut in_progress = store.begin_transaction().expect("began");

        // The good one succeeded!
        in_progress.ensure_vocabularies(&mut all_three).expect("expected success");
        in_progress.commit().expect("commit succeeded");
    }

    // Our custom migrator was able to hold on to state.
    assert_eq!(all_three.pre_food_version, 2);
    assert!(all_three.post_done);

    // Now we can fetch just the foods or movies that Sam likes, without having to filter on
    // :food/name or :movie/title -- they're separate relations.
    assert_eq!(store.q_once(r#"[:find [?f ...]
                                :where [?sam :person/name "Sam"]
                                       [?sam :food/likes ?f]]"#, None)
                    .into_coll_result()
                    .expect("success")
                    .len(),
               2);
    assert_eq!(store.q_once(r#"[:find [?m ...]
                                :where [?sam :person/name "Sam"]
                                       [?sam :movie/likes ?m]]"#, None)
                    .into_coll_result()
                    .expect("success")
                    .len(),
               2);

    // Find the height and name of anyone who likes both spice and movies released after 2012,
    // sorted by height.
    let q = r#"[:find ?name ?height
                :order (asc ?height)
                :where [?p :movie/likes ?m]
                       [?m :movie/year ?year]
                       [(> ?year 2012)]
                       [?p :food/likes ?f]
                       [?f :food/name "spice"]
                       [?p :person/height ?height]
                       [?p :person/name ?name]]"#;
    let r = store.q_once(q, None).into_rel_result().unwrap();
    let expected: RelResult<Binding> =
        vec![vec![TypedValue::typed_string("Sam"), TypedValue::Long(162)],
             vec![TypedValue::typed_string("Beth"), TypedValue::Long(172)]].into();
    assert_eq!(expected, r);

    // Find foods that Upstream Color fans like.
    let q = r#"[:find [?food ...]
                :order (asc ?food)
                :where [?uc :movie/title "Upstream Color"]
                       [?p :movie/likes ?uc]
                       [?p :food/likes ?f]
                       [?f :food/name ?food]]"#;
    let r = store.q_once(q, None).into_coll_result().unwrap();
    let expected: Vec<Binding> =
        vec![TypedValue::typed_string("spice").into(),
             TypedValue::typed_string("weird blue worms").into()];
    assert_eq!(expected, r);
}
