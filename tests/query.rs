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
extern crate mentat_core;
extern crate mentat_db;

use mentat_core::{
    TypedValue,
    ValueType,
};

use mentat::{
    NamespacedKeyword,
    QueryInputs,
    QueryResults,
    Variable,
    new_connection,
    q_once,
};

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
                         "[:find ?x ?ident :where [?x :db/ident ?ident]]", None, None)
        .expect("Query failed");
    let end = time::PreciseTime::now();

    // This will need to change each time we add a default ident.
    assert_eq!(37, results.len());

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
                         "[:find ?x . :where [?x :db/fulltext true]]", None, None)
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
                         "[:find ?ident . :where [24 :db/ident ?ident]]", None, None)
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
                         None, None)
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
                         "[:find [?e ...] :where [?e :db/ident _]]", None, None)
        .expect("Query failed");
    let end = time::PreciseTime::now();

    assert_eq!(37, results.len());

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
                         "[:find ?i . :in ?e :where [?e :db/ident ?i]]", inputs, None)
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
                         "[:find ?i . :in ?e :where [?e :db/ident ?i]]", inputs, None);

    match results {
        Result::Err(Error(ErrorKind::UnboundVariables(vars), _)) => {
            assert_eq!(vars, vec!["?e".to_string()].into_iter().collect());
        },
        _ => panic!("Expected unbound variables."),
    }
}
