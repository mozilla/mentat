// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate chrono;
extern crate time;

extern crate mentat;
extern crate mentat_core;
extern crate mentat_db;
extern crate mentat_query_algebrizer;       // For errors.

use std::str::FromStr;

use chrono::FixedOffset;

use mentat_core::{
    TypedValue,
    ValueType,
    UTC,
    Uuid,
};

use mentat::{
    NamespacedKeyword,
    PlainSymbol,
    QueryInputs,
    QueryResults,
    Variable,
    new_connection,
    q_once,
};

use mentat::conn::Conn;

use mentat::errors::{
    Error,
    ErrorKind,
};

#[test]
fn test_rel() {
    let mut c = new_connection("").expect("Couldn't open conn.");
    let db = mentat_db::db::ensure_current_version(&mut c).expect("Couldn't open DB.");

    // Rel.
    let start = time::PreciseTime::now();
    let results = q_once(&c, &db.schema,
                         "[:find ?x ?ident :where [?x :db/ident ?ident]]", None)
        .expect("Query failed");
    let end = time::PreciseTime::now();

    // This will need to change each time we add a default ident.
    assert_eq!(39, results.len());

    // Every row is a pair of a Ref and a Keyword.
    if let QueryResults::Rel(ref rel) = results {
        for r in rel {
            assert_eq!(r.len(), 2);
            assert!(r[0].matches_type(ValueType::Ref));
            assert!(r[1].matches_type(ValueType::Keyword));
        }
    } else {
        panic!("Expected rel.");
    }

    println!("{:?}", results);
    println!("Rel took {}µs", start.to(end).num_microseconds().unwrap());
}

#[test]
fn test_failing_scalar() {
    let mut c = new_connection("").expect("Couldn't open conn.");
    let db = mentat_db::db::ensure_current_version(&mut c).expect("Couldn't open DB.");

    // Scalar that fails.
    let start = time::PreciseTime::now();
    let results = q_once(&c, &db.schema,
                         "[:find ?x . :where [?x :db/fulltext true]]", None)
        .expect("Query failed");
    let end = time::PreciseTime::now();

    assert_eq!(0, results.len());

    if let QueryResults::Scalar(None) = results {
    } else {
        panic!("Expected failed scalar.");
    }

    println!("Failing scalar took {}µs", start.to(end).num_microseconds().unwrap());
}

#[test]
fn test_scalar() {
    let mut c = new_connection("").expect("Couldn't open conn.");
    let db = mentat_db::db::ensure_current_version(&mut c).expect("Couldn't open DB.");

    // Scalar that succeeds.
    let start = time::PreciseTime::now();
    let results = q_once(&c, &db.schema,
                         "[:find ?ident . :where [24 :db/ident ?ident]]", None)
        .expect("Query failed");
    let end = time::PreciseTime::now();

    assert_eq!(1, results.len());

    if let QueryResults::Scalar(Some(TypedValue::Keyword(ref rc))) = results {
        // Should be '24'.
        assert_eq!(&NamespacedKeyword::new("db.type", "keyword"), rc.as_ref());
        assert_eq!(24,
                   db.schema.get_entid(rc).unwrap());
    } else {
        panic!("Expected scalar.");
    }

    println!("{:?}", results);
    println!("Scalar took {}µs", start.to(end).num_microseconds().unwrap());
}

#[test]
fn test_tuple() {
    let mut c = new_connection("").expect("Couldn't open conn.");
    let db = mentat_db::db::ensure_current_version(&mut c).expect("Couldn't open DB.");

    // Tuple.
    let start = time::PreciseTime::now();
    let results = q_once(&c, &db.schema,
                         "[:find [?index ?cardinality]
                           :where [:db/txInstant :db/index ?index]
                                  [:db/txInstant :db/cardinality ?cardinality]]",
                         None)
        .expect("Query failed");
    let end = time::PreciseTime::now();

    assert_eq!(1, results.len());

    if let QueryResults::Tuple(Some(ref tuple)) = results {
        let cardinality_one = NamespacedKeyword::new("db.cardinality", "one");
        assert_eq!(tuple.len(), 2);
        assert_eq!(tuple[0], TypedValue::Boolean(true));
        assert_eq!(tuple[1], TypedValue::Ref(db.schema.get_entid(&cardinality_one).unwrap()));
    } else {
        panic!("Expected tuple.");
    }

    println!("{:?}", results);
    println!("Tuple took {}µs", start.to(end).num_microseconds().unwrap());
}

#[test]
fn test_coll() {
    let mut c = new_connection("").expect("Couldn't open conn.");
    let db = mentat_db::db::ensure_current_version(&mut c).expect("Couldn't open DB.");

    // Coll.
    let start = time::PreciseTime::now();
    let results = q_once(&c, &db.schema,
                         "[:find [?e ...] :where [?e :db/ident _]]", None)
        .expect("Query failed");
    let end = time::PreciseTime::now();

    assert_eq!(39, results.len());

    if let QueryResults::Coll(ref coll) = results {
        assert!(coll.iter().all(|item| item.matches_type(ValueType::Ref)));
    } else {
        panic!("Expected coll.");
    }

    println!("{:?}", results);
    println!("Coll took {}µs", start.to(end).num_microseconds().unwrap());
}

#[test]
fn test_inputs() {
    let mut c = new_connection("").expect("Couldn't open conn.");
    let db = mentat_db::db::ensure_current_version(&mut c).expect("Couldn't open DB.");

    // entids::DB_INSTALL_VALUE_TYPE = 5.
    let ee = (Variable::from_valid_name("?e"), TypedValue::Ref(5));
    let inputs = QueryInputs::with_value_sequence(vec![ee]);
    let results = q_once(&c, &db.schema,
                         "[:find ?i . :in ?e :where [?e :db/ident ?i]]", inputs)
                        .expect("query to succeed");

    if let QueryResults::Scalar(Some(TypedValue::Keyword(value))) = results {
        assert_eq!(value.as_ref(), &NamespacedKeyword::new("db.install", "valueType"));
    } else {
        panic!("Expected scalar.");
    }
}

/// Ensure that a query won't be run without all of its `:in` variables being bound.
#[test]
fn test_unbound_inputs() {
    let mut c = new_connection("").expect("Couldn't open conn.");
    let db = mentat_db::db::ensure_current_version(&mut c).expect("Couldn't open DB.");

    // Bind the wrong var by 'mistake'.
    let xx = (Variable::from_valid_name("?x"), TypedValue::Ref(5));
    let inputs = QueryInputs::with_value_sequence(vec![xx]);
    let results = q_once(&c, &db.schema,
                         "[:find ?i . :in ?e :where [?e :db/ident ?i]]", inputs);

    match results {
        Result::Err(Error(ErrorKind::UnboundVariables(vars), _)) => {
            assert_eq!(vars, vec!["?e".to_string()].into_iter().collect());
        },
        _ => panic!("Expected unbound variables."),
    }
}

#[test]
fn test_instants_and_uuids() {
    // We assume, perhaps foolishly, that the clocks on test machines won't lose more than an
    // hour while this test is running.
    let start = UTC::now() + FixedOffset::west(60 * 60);

    let mut c = new_connection("").expect("Couldn't open conn.");
    let mut conn = Conn::connect(&mut c).expect("Couldn't open DB.");
    conn.transact(&mut c, r#"[
        [:db/add "s" :db/ident :foo/uuid]
        [:db/add "s" :db/valueType :db.type/uuid]
        [:db/add "s" :db/cardinality :db.cardinality/one]
    ]"#).unwrap();
    conn.transact(&mut c, r#"[
        [:db/add "u" :foo/uuid #uuid "cf62d552-6569-4d1b-b667-04703041dfc4"]
    ]"#).unwrap();
    let r = conn.q_once(&mut c,
                        r#"[:find [?x ?u ?when]
                            :where [?x :foo/uuid ?u ?tx]
                                   [?tx :db/txInstant ?when]]"#, None);
    match r {
        Result::Ok(QueryResults::Tuple(Some(vals))) => {
            let mut vals = vals.into_iter();
            match (vals.next(), vals.next(), vals.next(), vals.next()) {
                (Some(TypedValue::Ref(e)),
                 Some(TypedValue::Uuid(u)),
                 Some(TypedValue::Instant(t)),
                 None) => {
                     assert!(e > 39);       // There are at least this many entities in the store.
                     assert_eq!(Ok(u), Uuid::from_str("cf62d552-6569-4d1b-b667-04703041dfc4"));
                     assert!(t > start);
                 },
                 _ => panic!("Unexpected results."),
            }
        },
        _ => panic!("Expected query to work."),
    }
}

#[test]
fn test_tx() {
    let mut c = new_connection("").expect("Couldn't open conn.");
    let mut conn = Conn::connect(&mut c).expect("Couldn't open DB.");
    conn.transact(&mut c, r#"[
        [:db/add "s" :db/ident :foo/uuid]
        [:db/add "s" :db/valueType :db.type/uuid]
        [:db/add "s" :db/cardinality :db.cardinality/one]
    ]"#).expect("successful transaction");

    let t = conn.transact(&mut c, r#"[
        [:db/add "u" :foo/uuid #uuid "cf62d552-6569-4d1b-b667-04703041dfc4"]
    ]"#).expect("successful transaction");

    conn.transact(&mut c, r#"[
        [:db/add "u" :foo/uuid #uuid "550e8400-e29b-41d4-a716-446655440000"]
    ]"#).expect("successful transaction");

    let r = conn.q_once(&mut c,
                        r#"[:find ?tx 
                            :where [?x :foo/uuid #uuid "cf62d552-6569-4d1b-b667-04703041dfc4" ?tx]]"#, None);
    match r {
        Result::Ok(QueryResults::Rel(ref v)) => {
            assert_eq!(*v, vec![
                vec![TypedValue::Ref(t.tx_id),]
            ]);
        },
        _ => panic!("Expected query to work."),
    }
}

#[test]
fn test_tx_as_input() {
    let mut c = new_connection("").expect("Couldn't open conn.");
    let mut conn = Conn::connect(&mut c).expect("Couldn't open DB.");
    conn.transact(&mut c, r#"[
        [:db/add "s" :db/ident :foo/uuid]
        [:db/add "s" :db/valueType :db.type/uuid]
        [:db/add "s" :db/cardinality :db.cardinality/one]
    ]"#).expect("successful transaction");
    conn.transact(&mut c, r#"[
        [:db/add "u" :foo/uuid #uuid "550e8400-e29b-41d4-a716-446655440000"]
    ]"#).expect("successful transaction");
    let t = conn.transact(&mut c, r#"[
        [:db/add "u" :foo/uuid #uuid "cf62d552-6569-4d1b-b667-04703041dfc4"]
    ]"#).expect("successful transaction");
    conn.transact(&mut c, r#"[
        [:db/add "u" :foo/uuid #uuid "267bab92-ee39-4ca2-b7f0-1163a85af1fb"]
    ]"#).expect("successful transaction");

    let tx = (Variable::from_valid_name("?tx"), TypedValue::Ref(t.tx_id));
    let inputs = QueryInputs::with_value_sequence(vec![tx]);
    let r = conn.q_once(&mut c,
                        r#"[:find ?uuid 
                            :in ?tx
                            :where [?x :foo/uuid ?uuid ?tx]]"#, inputs);
    match r {
        Result::Ok(QueryResults::Rel(ref v)) => {
            assert_eq!(*v, vec![
                vec![TypedValue::Uuid(Uuid::from_str("cf62d552-6569-4d1b-b667-04703041dfc4").expect("Valid UUID")),]
            ]);
        },
        _ => panic!("Expected query to work."),
    }
}

#[test]
fn test_fulltext() {
    let mut c = new_connection("").expect("Couldn't open conn.");
    let mut conn = Conn::connect(&mut c).expect("Couldn't open DB.");

    conn.transact(&mut c, r#"[
        [:db/add "a" :db/ident :foo/term]
        [:db/add "a" :db/valueType :db.type/string]
        [:db/add "a" :db/fulltext false]
        [:db/add "a" :db/cardinality :db.cardinality/many]

        [:db/add "s" :db/ident :foo/fts]
        [:db/add "s" :db/valueType :db.type/string]
        [:db/add "s" :db/fulltext true]
        [:db/add "s" :db/cardinality :db.cardinality/many]
    ]"#).unwrap();

    let v = conn.transact(&mut c, r#"[
        [:db/add "v" :foo/fts "hello darkness my old friend"]
        [:db/add "v" :foo/fts "I've come to talk with you again"]
    ]"#).unwrap().tempids.get("v").cloned().expect("v was mapped");

    let r = conn.q_once(&mut c,
                        r#"[:find [?x ?val ?score]
                            :where [(fulltext $ :foo/fts "darkness") [[?x ?val _ ?score]]]]"#, None);
    match r {
        Result::Ok(QueryResults::Tuple(Some(vals))) => {
            let mut vals = vals.into_iter();
            match (vals.next(), vals.next(), vals.next(), vals.next()) {
                (Some(TypedValue::Ref(x)),
                 Some(TypedValue::String(text)),
                 Some(TypedValue::Double(score)),
                 None) => {
                     assert_eq!(x, v);
                     assert_eq!(text.as_str(), "hello darkness my old friend");
                     assert_eq!(score, 0.0f64.into());
                 },
                 _ => panic!("Unexpected results."),
            }
        },
        _ => panic!("Expected query to work."),
    }

    let a = conn.transact(&mut c, r#"[[:db/add "a" :foo/term "talk"]]"#)
                .unwrap()
                .tempids
                .get("a").cloned()
                .expect("a was mapped");

    // If you use a non-constant search term, it must be bound earlier in the query.
    let query = r#"[:find ?x ?val
                    :where
                    [(fulltext $ :foo/fts ?term) [[?x ?val]]]
                    [?a :foo/term ?term]
                    ]"#;
    let r = conn.q_once(&mut c, query, None);
    match r {
        Err(Error(ErrorKind::QueryError(mentat_query_algebrizer::ErrorKind::InvalidArgument(PlainSymbol(s), ty, i)), _)) => {
            assert_eq!(s, "fulltext");
            assert_eq!(ty, "string");
            assert_eq!(i, 2);
        },
        _ => panic!("Expected query to fail."),
    }

    // Bound to the wrong type? Error.
    let query = r#"[:find ?x ?val
                    :where
                    [?a :foo/term ?term]
                    [(fulltext $ :foo/fts ?a) [[?x ?val]]]]"#;
    let r = conn.q_once(&mut c, query, None);
    match r {
        Err(Error(ErrorKind::QueryError(mentat_query_algebrizer::ErrorKind::InvalidArgument(PlainSymbol(s), ty, i)), _)) => {
            assert_eq!(s, "fulltext");
            assert_eq!(ty, "string");
            assert_eq!(i, 2);
        },
        _ => panic!("Expected query to fail."),
    }

    // If it's bound, and the right type, it'll work!
    let query = r#"[:find ?x ?val
                    :in ?a
                    :where
                    [?a :foo/term ?term]
                    [(fulltext $ :foo/fts ?term) [[?x ?val]]]]"#;
    let inputs = QueryInputs::with_value_sequence(vec![(Variable::from_valid_name("?a"), TypedValue::Ref(a))]);
    let r = conn.q_once(&mut c, query, inputs);
    match r {
        Result::Ok(QueryResults::Rel(rels)) => {
            assert_eq!(rels, vec![
                vec![TypedValue::Ref(v),
                     TypedValue::String("I've come to talk with you again".to_string().into()),
                ]
            ]);
        },
        _ => panic!("Expected query to work."),
    }
}

#[test]
fn test_instant_range_query() {
    let mut c = new_connection("").expect("Couldn't open conn.");
    let mut conn = Conn::connect(&mut c).expect("Couldn't open DB.");

    conn.transact(&mut c, r#"[
        [:db/add "a" :db/ident :foo/date]
        [:db/add "a" :db/valueType :db.type/instant]
        [:db/add "a" :db/cardinality :db.cardinality/one]
    ]"#).unwrap();

    let ids = conn.transact(&mut c, r#"[
        [:db/add "b" :foo/date #inst "2016-01-01T11:00:00.000Z"]
        [:db/add "c" :foo/date #inst "2016-06-01T11:00:01.000Z"]
        [:db/add "d" :foo/date #inst "2017-01-01T11:00:02.000Z"]
        [:db/add "e" :foo/date #inst "2017-06-01T11:00:03.000Z"]
    ]"#).unwrap().tempids;

    let r = conn.q_once(&mut c,
                        r#"[:find [?x ...]
                            :order (asc ?date)
                            :where
                            [?x :foo/date ?date]
                            [(< ?date #inst "2017-01-01T11:00:02.000Z")]]"#, None);
    match r {
        Result::Ok(QueryResults::Coll(vals)) => {
            assert_eq!(vals,
                       vec![TypedValue::Ref(*ids.get("b").unwrap()),
                            TypedValue::Ref(*ids.get("c").unwrap())]);
        },
        _ => panic!("Expected query to work."),
    }
}
