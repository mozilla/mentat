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
    Utc,
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
    let start = Utc::now() + FixedOffset::west(60 * 60);

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
        Result::Ok(r) => panic!("Unexpected results {:?}.", r),
        Result::Err(e) => panic!("Expected query to work, got {:?}.", e),
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

#[test]
fn test_aggregation_implicit_grouping() {
    let mut c = new_connection("").expect("Couldn't open conn.");
    let mut conn = Conn::connect(&mut c).expect("Couldn't open DB.");

    conn.transact(&mut c, r#"[
        [:db/add "a" :db/ident :foo/score]
        [:db/add "a" :db/valueType :db.type/long]
        [:db/add "a" :db/cardinality :db.cardinality/one]
        [:db/add "b" :db/ident :foo/name]
        [:db/add "b" :db/valueType :db.type/string]
        [:db/add "b" :db/cardinality :db.cardinality/one]
        [:db/add "c" :db/ident :foo/is-vegetarian]
        [:db/add "c" :db/valueType :db.type/boolean]
        [:db/add "c" :db/cardinality :db.cardinality/one]
        [:db/add "d" :db/ident :foo/play]
        [:db/add "d" :db/valueType :db.type/ref]
        [:db/add "d" :db/cardinality :db.cardinality/many]
        [:db/add "d" :db/unique :db.unique/value]
    ]"#).unwrap();

    let ids = conn.transact(&mut c, r#"[
        [:db/add "a" :foo/name "Alice"]
        [:db/add "b" :foo/name "Beli"]
        [:db/add "c" :foo/name "Carlos"]
        [:db/add "d" :foo/name "Diana"]
        [:db/add "a" :foo/is-vegetarian true]
        [:db/add "b" :foo/is-vegetarian true]
        [:db/add "c" :foo/is-vegetarian false]
        [:db/add "d" :foo/is-vegetarian false]
        [:db/add "aa" :foo/score 14]
        [:db/add "ab" :foo/score 99]
        [:db/add "ac" :foo/score 14]
        [:db/add "ba" :foo/score 22]
        [:db/add "bb" :foo/score 11]
        [:db/add "ca" :foo/score 42]
        [:db/add "da" :foo/score 5]
        [:db/add "db" :foo/score 28]
        [:db/add "d"  :foo/play "da"]
        [:db/add "d"  :foo/play "db"]
        [:db/add "a"  :foo/play "aa"]
        [:db/add "a"  :foo/play "ab"]
        [:db/add "a"  :foo/play "ac"]
        [:db/add "b"  :foo/play "ba"]
        [:db/add "b"  :foo/play "bb"]
        [:db/add "c"  :foo/play "ca"]
    ]"#).unwrap().tempids;

    // We can combine these aggregates.
    let r = conn.q_once(&mut c,
                        r#"[:find ?x ?name (max ?score) (count ?score) (avg ?score)
                            :where
                            [?x :foo/name ?name]
                            [?x :foo/play ?game]
                            [?game :foo/score ?score]
                            ]"#, None);
    match r {
        Result::Ok(QueryResults::Rel(vals)) => {
            assert_eq!(vals,
                vec![
                    vec![TypedValue::Ref(ids.get("a").cloned().unwrap()),
                         TypedValue::String("Alice".to_string().into()),
                         TypedValue::Long(99),
                         TypedValue::Long(3),
                         TypedValue::Double((127f64 / 3f64).into())],
                    vec![TypedValue::Ref(ids.get("b").cloned().unwrap()),
                         TypedValue::String("Beli".to_string().into()),
                         TypedValue::Long(22),
                         TypedValue::Long(2),
                         TypedValue::Double((33f64 / 2f64).into())],
                    vec![TypedValue::Ref(ids.get("c").cloned().unwrap()),
                         TypedValue::String("Carlos".to_string().into()),
                         TypedValue::Long(42),
                         TypedValue::Long(1),
                         TypedValue::Double(42f64.into())],
                    vec![TypedValue::Ref(ids.get("d").cloned().unwrap()),
                         TypedValue::String("Diana".to_string().into()),
                         TypedValue::Long(28),
                         TypedValue::Long(2),
                         TypedValue::Double((33f64 / 2f64).into())]]);
        },
        Result::Ok(x) => panic!("Got unexpected results {:?}", x),
        Result::Err(e) => panic!("Expected query to work: got {:?}", e),
    }
}

// TODO: this can't be phrased in Datalog!
/*
#[test]
fn test_corresponding_row_value_aggregation() {

    // Who's youngest, via min?
    let r = conn.q_once(&mut c,
                        r#"[:find [?name (min ?age)]
                            :where
                            [?x :foo/age ?age]
                            [?x :foo/name ?name]]"#, None);
    match r {
        Result::Ok(QueryResults::Tuple(Some(vals))) => {
            assert_eq!(vals,
                       vec![TypedValue::String("Alice".to_string().into()),
                            TypedValue::Long(14)]);
        },
        _ => panic!("Expected query to work."),
    }

    // Who's oldest, via max?
    let r = conn.q_once(&mut c,
                        r#"[:find [?name (max ?age)]
                            :where
                            [?x :foo/age ?age]
                            [?x :foo/name ?name]]"#, None);
    match r {
        Result::Ok(QueryResults::Tuple(Some(vals))) => {
            assert_eq!(vals,
                       vec![TypedValue::String("Carlos".to_string().into()),
                            TypedValue::Long(42)]);
        },
        _ => panic!("Expected query to work."),
    }
}
*/

#[test]
fn test_simple_aggregation() {
    let mut c = new_connection("").expect("Couldn't open conn.");
    let mut conn = Conn::connect(&mut c).expect("Couldn't open DB.");

    conn.transact(&mut c, r#"[
        [:db/add "a" :db/ident :foo/age]
        [:db/add "a" :db/valueType :db.type/long]
        [:db/add "a" :db/cardinality :db.cardinality/one]
        [:db/add "b" :db/ident :foo/name]
        [:db/add "b" :db/valueType :db.type/string]
        [:db/add "b" :db/cardinality :db.cardinality/one]
        [:db/add "c" :db/ident :foo/is-vegetarian]
        [:db/add "c" :db/valueType :db.type/boolean]
        [:db/add "c" :db/cardinality :db.cardinality/one]
    ]"#).unwrap();

    let ids = conn.transact(&mut c, r#"[
        [:db/add "a" :foo/name "Alice"]
        [:db/add "b" :foo/name "Beli"]
        [:db/add "c" :foo/name "Carlos"]
        [:db/add "d" :foo/name "Diana"]
        [:db/add "a" :foo/is-vegetarian true]
        [:db/add "b" :foo/is-vegetarian true]
        [:db/add "c" :foo/is-vegetarian false]
        [:db/add "d" :foo/is-vegetarian false]
        [:db/add "a" :foo/age 14]
        [:db/add "b" :foo/age 22]
        [:db/add "c" :foo/age 42]
        [:db/add "d" :foo/age 28]
    ]"#).unwrap().tempids;

    // What are the oldest and youngest ages?
    let r = conn.q_once(&mut c,
                        r#"[:find [(min ?age) (max ?age)]
                            :where
                            [_ :foo/age ?age]]"#, None);
    match r {
        Result::Ok(QueryResults::Tuple(Some(vals))) => {
            assert_eq!(vals,
                       vec![TypedValue::Long(14),
                            TypedValue::Long(42)]);
        },
        _ => panic!("Expected query to work."),
    }

    // Who's youngest, via order?
    let r = conn.q_once(&mut c,
                        r#"[:find [?name ?age]
                            :order (asc ?age)
                            :where
                            [?x :foo/age ?age]
                            [?x :foo/name ?name]]"#, None);
    match r {
        Result::Ok(QueryResults::Tuple(Some(vals))) => {
            assert_eq!(vals,
                       vec![TypedValue::String("Alice".to_string().into()),
                            TypedValue::Long(14)]);
        },
        Result::Ok(r) => panic!("Unexpected results {:?}", r),
        Result::Err(e) => panic!("Expected query to work, got {:?}", e),
    }

    // Who's oldest, via order?
    let r = conn.q_once(&mut c,
                        r#"[:find [?name ?age]
                            :order (desc ?age)
                            :where
                            [?x :foo/age ?age]
                            [?x :foo/name ?name]]"#, None);
    match r {
        Result::Ok(QueryResults::Tuple(Some(vals))) => {
            assert_eq!(vals,
                       vec![TypedValue::String("Carlos".to_string().into()),
                            TypedValue::Long(42)]);
        },
        _ => panic!("Expected query to work."),
    }

    // What's the average age?
    let r = conn.q_once(&mut c,
                        r#"[:find (avg ?age) .
                            :where
                            [_ :foo/age ?age]]"#, None);
    match r {
        Result::Ok(QueryResults::Scalar(Some(sum))) => {
            assert_eq!(sum, TypedValue::Double(26.5f64.into()));
        },
        _ => panic!("Expected query to work."),
    }

    // What's the total age?
    let r = conn.q_once(&mut c,
                        r#"[:find (sum ?age) .
                            :where
                            [_ :foo/age ?age]]"#, None);
    match r {
        Result::Ok(QueryResults::Scalar(Some(sum))) => {
            assert_eq!(sum, TypedValue::Long(106));
        },
        _ => panic!("Expected query to work."),
    }

    // How many distinct names are there?
    let r = conn.q_once(&mut c,
                        r#"[:find (count ?name) .
                            :where
                            [_ :foo/name ?name]]"#, None);
    match r {
        Result::Ok(QueryResults::Scalar(Some(count))) => {
            assert_eq!(count, TypedValue::Long(4));
        },
        _ => panic!("Expected query to work."),
    }

    // We can use constraints, too.
    // What's the average age of adults?
    let r = conn.q_once(&mut c,
                        r#"[:find [(avg ?age) (count ?age)]
                            :where
                            [_ :foo/age ?age]
                            [(>= ?age 18)]]"#, None);
    match r {
        Result::Ok(QueryResults::Tuple(Some(vals))) => {
            assert_eq!(vals, vec![TypedValue::Double((92f64 / 3f64).into()),
                                  TypedValue::Long(3)]);
        },
        Result::Ok(x) => panic!("Got unexpected results {:?}", x),
        Result::Err(e) => panic!("Expected query to work: got {:?}", e),
    }

    // Who's oldest, vegetarians or not?
    let r = conn.q_once(&mut c,
                        r#"[:find ?veg (max ?age)
                            :where
                            [?p :foo/age ?age]
                            [?p :foo/is-vegetarian ?veg]]"#, None);
    match r {
        Result::Ok(QueryResults::Rel(vals)) => {
            assert_eq!(vals, vec![
                vec![TypedValue::Boolean(false), TypedValue::Long(42)],
                vec![TypedValue::Boolean(true), TypedValue::Long(22)],
            ]);
        },
        Result::Ok(x) => panic!("Got unexpected results {:?}", x),
        Result::Err(e) => panic!("Expected query to work: got {:?}", e),
    }
}
