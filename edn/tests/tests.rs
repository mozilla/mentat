// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate edn;
extern crate num;
extern crate ordered_float;

use std::collections::{BTreeSet, BTreeMap, LinkedList};
use std::iter::FromIterator;
use num::bigint::ToBigInt;
use num::traits::{Zero, One};
use ordered_float::OrderedFloat;
use edn::symbols;
use edn::types::Value;
use edn::types::Value::*;
use edn::parse::*;
use edn::utils;

// Helper for making wrapped keywords with a namespace.
fn k_ns(ns: &str, name: &str) -> Value {
    return NamespacedKeyword(symbols::NamespacedKeyword::new(ns, name));
}

// Helper for making wrapped keywords without a namespace.
fn k_plain(name: &str) -> Value {
    return Keyword(symbols::Keyword::new(name));
}

fn s_plain(name: &str) -> Value {
    return PlainSymbol(symbols::PlainSymbol::new(name));
}

#[test]
fn test_nil() {
    assert_eq!(nil("nil").unwrap(), Nil);

    assert!(nil("true").is_err());
}

#[test]
fn test_boolean() {
    assert_eq!(boolean("true").unwrap(), Boolean(true));
    assert_eq!(boolean("false").unwrap(), Boolean(false));

    assert!(boolean("nil").is_err());
}

#[test]
fn test_integer() {
    assert_eq!(integer("0").unwrap(), Integer(0i64));
    assert_eq!(integer("1").unwrap(), Integer(1i64));
    assert_eq!(integer("999").unwrap(), Integer(999i64));
    assert_eq!(integer("-999").unwrap(), Integer(-999i64));

    assert!(integer("nil").is_err());
}

#[test]
fn test_bigint() {
    let max_i64 = i64::max_value().to_bigint().unwrap();
    let bigger = &max_i64 * &max_i64;

    assert_eq!(bigint("0N").unwrap(), BigInteger(Zero::zero()));
    assert_eq!(bigint("1N").unwrap(), BigInteger(One::one()));
    assert_eq!(bigint("9223372036854775807N").unwrap(), BigInteger(max_i64));
    assert_eq!(bigint("85070591730234615847396907784232501249N").unwrap(), BigInteger(bigger));

    assert!(bigint("nil").is_err());
}

#[test]
fn test_float() {
    assert_eq!(float("111.222").unwrap(), Float(OrderedFloat(111.222f64)));
    assert_eq!(float("3e4").unwrap(), Float(OrderedFloat(3e4f64)));
    assert_eq!(float("-55e-66").unwrap(), Float(OrderedFloat(-55e-66f64)));
    assert_eq!(float("77.88e99").unwrap(), Float(OrderedFloat(77.88e99f64)));
    assert_eq!(float("-9.9E-9").unwrap(), Float(OrderedFloat(-9.9E-9f64)));

    assert!(float("nil").is_err());
}

#[test]
fn test_text() {
    assert_eq!(text("\"hello world\"").unwrap(), Text("hello world".to_string()));
    assert_eq!(text("\"\"").unwrap(), Text("".to_string()));

    assert!(text("\"").is_err());
    assert!(text("nil").is_err());
}

#[test]
fn test_symbol() {
    assert_eq!(symbol("$").unwrap(), s_plain("$"));
    assert_eq!(symbol(".").unwrap(), s_plain("."));
    //assert_eq!(symbol("r_r").unwrap(), s_plain("r_r"));
    //assert_eq!(symbol("$symbol").unwrap(), s_plain("$symbol"));
    //assert_eq!(symbol("hello").unwrap(), s_plain("hello"));
}

#[test]
fn test_keyword() {
    assert_eq!(keyword(":hello/world").unwrap(), k_ns("hello", "world"));

    assert_eq!(keyword(":symbol").unwrap(), k_plain("symbol"));
    assert_eq!(keyword(":hello").unwrap(), k_plain("hello"));
}

#[test]
fn test_value() {
    let max_i64 = i64::max_value().to_bigint().unwrap();
    let bigger = &max_i64 * &max_i64;

    assert_eq!(value("nil").unwrap(), Nil);
    assert_eq!(value("true").unwrap(), Boolean(true));
    assert_eq!(value("1").unwrap(), Integer(1i64));
    assert_eq!(value("\"hello world\"").unwrap(), Text("hello world".to_string()));
    assert_eq!(value("$").unwrap(), s_plain("$"));
    assert_eq!(value(".").unwrap(), s_plain("."));
    assert_eq!(value("$symbol").unwrap(), s_plain("$symbol"));
    assert_eq!(value(":hello").unwrap(), k_plain("hello"));
    assert_eq!(value("[1]").unwrap(), Vector(vec![Integer(1)]));
    assert_eq!(value("111.222").unwrap(), Float(OrderedFloat(111.222f64)));
    assert_eq!(value("85070591730234615847396907784232501249N").unwrap(), BigInteger(bigger));
}

#[test]
fn test_vector() {
    let max_i64 = i64::max_value().to_bigint().unwrap();
    let bigger = &max_i64 * &max_i64;

    let test = "[]";
    let value = Vector(vec![
    ]);
    assert_eq!(vector(test).unwrap(), value);

    let test = "[1]";
    let value = Vector(vec![
        Integer(1),
    ]);
    assert_eq!(vector(test).unwrap(), value);

    let test = "[nil]";
    let value = Vector(vec![
        Nil,
    ]);
    assert_eq!(vector(test).unwrap(), value);

    let test = "[1 2]";
    let value = Vector(vec![
        Integer(1),
        Integer(2),
    ]);
    assert_eq!(vector(test).unwrap(), value);

    let test = "[1 2 3.4 85070591730234615847396907784232501249N]";
    let value = Vector(vec![
        Integer(1),
        Integer(2),
        Float(OrderedFloat(3.4f64)),
        BigInteger(bigger),
    ]);
    assert_eq!(vector(test).unwrap(), value);

    let test = "[1 0 nil \"nil\"]";
    let value = Vector(vec![
        Integer(1),
        Integer(0),
        Nil,
        Text("nil".to_string()),
    ]);
    assert_eq!(vector(test).unwrap(), value);

    let test = "[1 [0 nil] \"nil\"]";
    let value = Vector(vec![
        Integer(1),
        Vector(vec![
            Integer(0),
            Nil,
        ]),
        Text("nil".to_string()),
    ]);
    assert_eq!(vector(test).unwrap(), value);

    assert!(vector("[").is_err());
    assert!(vector("(").is_err());
    assert!(vector("1)").is_err());
    assert!(vector("(1 (2 nil) \"hi\"").is_err());
}

#[test]
fn test_list() {
    let test = "()";
    let value = List(LinkedList::from_iter(vec![
    ]));
    assert_eq!(list(test).unwrap(), value);

    let test = "(1)";
    let value = List(LinkedList::from_iter(vec![
        Integer(1),
    ]));
    assert_eq!(list(test).unwrap(), value);

    let test = "(nil)";
    let value = List(LinkedList::from_iter(vec![
        Nil,
    ]));
    assert_eq!(list(test).unwrap(), value);

    let test = "(1 2)";
    let value = List(LinkedList::from_iter(vec![
        Integer(1),
        Integer(2),
    ]));
    assert_eq!(list(test).unwrap(), value);

    let test = "(1 2 3.4)";
    let value = List(LinkedList::from_iter(vec![
        Integer(1),
        Integer(2),
        Float(OrderedFloat(3.4f64)),
    ]));
    assert_eq!(list(test).unwrap(), value);

    let test = "(1 0 nil \"nil\")";
    let value = List(LinkedList::from_iter(vec![
        Integer(1),
        Integer(0),
        Nil,
        Text("nil".to_string()),
    ]));
    assert_eq!(list(test).unwrap(), value);

    let test = "(1 (0 nil) \"nil\")";
    let value = List(LinkedList::from_iter(vec![
        Integer(1),
        List(LinkedList::from_iter(vec![
            Integer(0),
            Nil,
        ])),
        Text("nil".to_string()),
    ]));
    assert_eq!(list(test).unwrap(), value);

    assert!(list("[").is_err());
    assert!(list("(").is_err());
    assert!(list("1)").is_err());
    assert!(list("(1 (2 nil) \"hi\"").is_err());
}

#[test]
fn test_set() {
    let test = "#{}";
    let value = Set(BTreeSet::from_iter(vec![
    ]));
    assert_eq!(set(test).unwrap(), value);

    let test = "#{1}";
    let value = Set(BTreeSet::from_iter(vec![
        Integer(1),
    ]));
    assert_eq!(set(test).unwrap(), value);

    let test = "#{nil}";
    let value = Set(BTreeSet::from_iter(vec![
        Nil,
    ]));
    assert_eq!(set(test).unwrap(), value);

    // These tests assume the implementation of Ord for Value, (specifically the sort order which
    // isn't part of the spec) ideally we'd just test for set contents, however since the API
    // (BTreeSet) assumes sorting this seems pointless.
    // See the notes in types.rs for why we use BTreeSet rather than HashSet
    let test = "#{2 1}";
    let value = Set(BTreeSet::from_iter(vec![
        Integer(1),
        Integer(2),
    ]));
    assert_eq!(set(test).unwrap(), value);

    let test = "#{3.4 2 1}";
    let value = Set(BTreeSet::from_iter(vec![
        Integer(1),
        Integer(2),
        Float(OrderedFloat(3.4f64)),
    ]));
    assert_eq!(set(test).unwrap(), value);

    let test = "#{1 0 nil \"nil\"}";
    let value = Set(BTreeSet::from_iter(vec![
        Nil,
        Integer(0),
        Integer(1),
        Text("nil".to_string()),
    ]));
    assert_eq!(set(test).unwrap(), value);

    let test = "#{1 #{0 nil} \"nil\"}";
    let value = Set(BTreeSet::from_iter(vec![
        Integer(1),
        Set(BTreeSet::from_iter(vec![
            Nil,
            Integer(0),
        ])),
        Text("nil".to_string()),
    ]));
    assert_eq!(set(test).unwrap(), value);

    assert!(set("#{").is_err());
    assert!(set("}").is_err());
    assert!(set("1}").is_err());
    assert!(set("#{1 #{2 nil} \"hi\"").is_err());
}

#[test]
fn test_map() {
    let test = "{}";
    let value = Map(BTreeMap::from_iter(vec![
    ]));
    assert_eq!(map(test).unwrap(), value);

    let test = "{\"a\" 1}";
    let value = Map(BTreeMap::from_iter(vec![
        (Text("a".to_string()), Integer(1)),
    ]));
    assert_eq!(map(test).unwrap(), value);

    let test = "{nil 1, \"b\" 2}";
    let value = Map(BTreeMap::from_iter(vec![
        (Nil, Integer(1)),
        (Text("b".to_string()), Integer(2)),
    ]));
    assert_eq!(map(test).unwrap(), value);

    let test = "{nil 1, \"b\" 2, \"a\" 3}";
    let value = Map(BTreeMap::from_iter(vec![
        (Nil, Integer(1)),
        (Text("a".to_string()), Integer(3)),
        (Text("b".to_string()), Integer(2)),
    ]));
    assert_eq!(map(test).unwrap(), value);

    let test = "{:a 1, $b {:b/a nil, :b/b #{nil 5}}, c [1 2], d (3 4)}";
    let value = Map(
        BTreeMap::from_iter(
            vec![
            (Keyword(symbols::Keyword::new("a")), Integer(1)),
            (s_plain("$b"), Map(BTreeMap::from_iter(vec![
                                                               (k_ns("b", "a"), Nil),

                                                               (k_ns("b", "b"),
                                                               Set(BTreeSet::from_iter(vec![Nil, Integer(5),]))),
            ]))),
            (s_plain("c"), Vector(vec![Integer(1), Integer(2),])),
            (s_plain("d"), List(LinkedList::from_iter(vec![Integer(3), Integer(4),]))),
            ]));
    assert_eq!(map(test).unwrap(), value);

    assert!(map("#{").is_err());
    assert!(map("}").is_err());
    assert!(map("1}").is_err());
    assert!(map("#{1 #{2 nil} \"hi\"").is_err());
}

/// The test_query_* functions contain the queries taken from the old Clojure implementation of Mentat.
/// 2 changes have been applied, which should be checked and maybe fixed
/// TODO: Decide if these queries should be placed in a vector wrapper. Is that implied?
/// Secondly, see note in test_query_starred_pages on the use of '
#[test]
fn test_query_active_sessions() {
    let test = "[
        :find ?id ?reason ?ts
        :in $
        :where
            [?id :session/startReason ?reason ?tx]
            [?tx :db/txInstant ?ts]
            (not-join [?id] [?id :session/endReason _])
    ]";

    let reply = Vector(vec![
        k_plain("find"),
        s_plain("?id"),
        s_plain("?reason"),
        s_plain("?ts"),
        k_plain("in"),
        s_plain("$"),
        k_plain("where"),
        Vector(vec![
            s_plain("?id"),
            k_ns("session", "startReason"),
            s_plain("?reason"),
            s_plain("?tx"),
        ]),
        Vector(vec![
            s_plain("?tx"),
            k_ns("db", "txInstant"),
            s_plain("?ts"),
        ]),
        List(LinkedList::from_iter(vec![
            s_plain("not-join"),
            Vector(vec![
                s_plain("?id"),
            ]),
            Vector(vec![
                s_plain("?id"),
                k_ns("session", "endReason"),
                s_plain("_"),
            ]),
        ])),
    ]);
    assert_eq!(value(test).unwrap(), reply);
}

#[test]
fn test_query_ended_sessions() {
    let test = "[
        :find ?id ?endReason ?ts
        :in $
        :where
            [?id :session/endReason ?endReason ?tx]
            [?tx :db/txInstant ?ts]
    ]";

    let reply = Vector(vec![
        k_plain("find"),
        s_plain("?id"),
        s_plain("?endReason"),
        s_plain("?ts"),
        k_plain("in"),
        s_plain("$"),
        k_plain("where"),
        Vector(vec![
            s_plain("?id"),
            k_ns("session", "endReason"),
            s_plain("?endReason"),
            s_plain("?tx"),
        ]),
        Vector(vec![
            s_plain("?tx"),
            k_ns("db", "txInstant"),
            s_plain("?ts"),
        ]),
    ]);
    assert_eq!(value(test).unwrap(), reply);
}

#[test]
fn test_query_starred_pages() {
    // TODO: The original query had added "'" like `:find '[?url` and `since '[$ ?since] '[$]`
    let test = "[
        :find [?url ?title ?starredOn]
        :in (if since [$ ?since] [$])
        :where where
    ]";

    let reply = Vector(vec![
        k_plain("find"),
        Vector(vec![
            s_plain("?url"),
            s_plain("?title"),
            s_plain("?starredOn"),
        ]),
        k_plain("in"),
        List(LinkedList::from_iter(vec![
            s_plain("if"),
            s_plain("since"),
            Vector(vec![
                s_plain("$"),
                s_plain("?since"),
            ]),
            Vector(vec![
                s_plain("$"),
            ]),
        ])),
        k_plain("where"),
        s_plain("where"),
    ]);
    assert_eq!(value(test).unwrap(), reply);
}

#[test]
fn test_query_saved_pages() {
    let test = "[
        :find ?page ?url ?title ?excerpt
        :in $
        :where
            [?save :save/page ?page]
            [?save :save/savedAt ?instant]
            [?page :page/url ?url]
            [(get-else $ ?save :save/title \"\") ?title]
            [(get-else $ ?save :save/excerpt \"\") ?excerpt]
    ]";

    let reply = Vector(vec![
        k_plain("find"),
        s_plain("?page"),
        s_plain("?url"),
        s_plain("?title"),
        s_plain("?excerpt"),
        k_plain("in"),
        s_plain("$"),
        k_plain("where"),
        Vector(vec![
            s_plain("?save"),
            k_ns("save", "page"),
            s_plain("?page"),
        ]),
        Vector(vec![
            s_plain("?save"),
            k_ns("save", "savedAt"),
            s_plain("?instant"),
        ]),
        Vector(vec![
            s_plain("?page"),
            k_ns("page", "url"),
            s_plain("?url"),
        ]),
        Vector(vec![
            List(LinkedList::from_iter(vec![
                s_plain("get-else"),
                s_plain("$"),
                s_plain("?save"),
                k_ns("save", "title"),
                Text("".to_string()),
            ])),
            s_plain("?title"),
        ]),
        Vector(vec![
            List(LinkedList::from_iter(vec![
                s_plain("get-else"),
                s_plain("$"),
                s_plain("?save"),
                k_ns("save", "excerpt"),
                Text("".to_string()),
            ])),
            s_plain("?excerpt"),
        ]),
    ]);
    assert_eq!(value(test).unwrap(), reply);
}

#[test]
fn test_query_pages_matching_string_1() {
    /*
    // Original query
    :find '[?url ?title]
    :in '[$]
    :where [
        [(list 'fulltext '$ #{:page/url :page/title} string) '[[?page]]]
        '[(get-else $ ?page :page/url \"\") ?url]
        '[(get-else $ ?page :page/title \"\") ?title]
    ]
    */
    let test = "[
        :find [?url ?title]
        :in [$]
        :where [
            [(list fulltext $ #{:page/url :page/title} string) [[?page]]]
            [(get-else $ ?page :page/url \"\") ?url]
            [(get-else $ ?page :page/title \"\") ?title]
        ]
    ]";

    let reply = Vector(vec![
        k_plain("find"),
        Vector(vec![
            s_plain("?url"),
            s_plain("?title"),
        ]),
        k_plain("in"),
        Vector(vec![
            s_plain("$"),
        ]),
        k_plain("where"),
        Vector(vec![
            Vector(vec![
                List(LinkedList::from_iter(vec![
                    s_plain("list"),
                    s_plain("fulltext"),
                    s_plain("$"),
                    Set(BTreeSet::from_iter(vec![
                        k_ns("page", "url"),
                        k_ns("page", "title"),
                    ])),
                    s_plain("string"),
                ])),
                Vector(vec![
                    Vector(vec![
                        s_plain("?page"),
                    ]),
                ]),
            ]),
            Vector(vec![
                List(LinkedList::from_iter(vec![
                    s_plain("get-else"),
                    s_plain("$"),
                    s_plain("?page"),
                    k_ns("page", "url"),
                    Text("".to_string()),
                ])),
                s_plain("?url"),
            ]),
            Vector(vec![
                List(LinkedList::from_iter(vec![
                    s_plain("get-else"),
                    s_plain("$"),
                    s_plain("?page"),
                    k_ns("page", "title"),
                    Text("".to_string()),
                ])),
                s_plain("?title"),
            ]),
        ]),
    ]);
    assert_eq!(value(test).unwrap(), reply);
}

#[test]
fn test_query_pages_matching_string_2() {
    /*
    // Original query
    :find '[?url ?title ?excerpt]
    :in '[$]
    :where [
        [(list 'fulltext '$ #{:save/title :save/excerpt :save/content} string) '[[?save]]]
        '[?save :save/page ?page]
        '[?page :page/url ?url]
        '[(get-else $ ?save :save/title \"\") ?title]
        '[(get-else $ ?save :save/excerpt \"\") ?excerpt]
    ]
    */
    let test = "[
        :find [?url ?title ?excerpt]
        :in [$]
        :where [
            [(list fulltext $ #{:save/title :save/excerpt :save/content} string) [[?save]]]
            [?save :save/page ?page]
            [?page :page/url ?url]
            [(get-else $ ?save :save/title \"\") ?title]
            [(get-else $ ?save :save/excerpt \"\") ?excerpt]
        ]
    ]";

    let reply = Vector(vec![
        k_plain("find"),
        Vector(vec![
            s_plain("?url"),
            s_plain("?title"),
            s_plain("?excerpt"),
        ]),
        k_plain("in"),
        Vector(vec![
            s_plain("$"),
        ]),
        k_plain("where"),
        Vector(vec![
            Vector(vec![
                List(LinkedList::from_iter(vec![
                    s_plain("list"),
                    s_plain("fulltext"),
                    s_plain("$"),
                    Set(BTreeSet::from_iter(vec![
                        k_ns("save", "title"),
                        k_ns("save", "excerpt"),
                        k_ns("save", "content"),
                    ])),
                    s_plain("string"),
                ])),
                Vector(vec![
                    Vector(vec![
                        s_plain("?save"),
                    ]),
                ]),
            ]),
            Vector(vec![
                s_plain("?save"),
                k_ns("save", "page"),
                s_plain("?page"),
            ]),
            Vector(vec![
                s_plain("?page"),
                k_ns("page", "url"),
                s_plain("?url"),
            ]),
            Vector(vec![
                List(LinkedList::from_iter(vec![
                    s_plain("get-else"),
                    s_plain("$"),
                    s_plain("?save"),
                    k_ns("save", "title"),
                    Text("".to_string()),
                ])),
                s_plain("?title"),
            ]),
            Vector(vec![
                List(LinkedList::from_iter(vec![
                    s_plain("get-else"),
                    s_plain("$"),
                    s_plain("?save"),
                    k_ns("save", "excerpt"),
                    Text("".to_string()),
                ])),
                s_plain("?excerpt"),
            ]),
        ]),
    ]);
    assert_eq!(value(test).unwrap(), reply);
}

#[test]
fn test_query_visited() {
    /*
    // Original query
    :find '[?url ?title (max ?time)]
    :in (if since '[$ ?since] '[$])
    :where where
    */
    let test = "[
        :find [?url ?title (max ?time)]
        :in (if since [$ ?since] [$])
        :where where
    ]";

    let reply = Vector(vec![
        k_plain("find"),
        Vector(vec![
            s_plain("?url"),
            s_plain("?title"),
            List(LinkedList::from_iter(vec![
                s_plain("max"),
                s_plain("?time"),
            ])),
        ]),
        k_plain("in"),
        List(LinkedList::from_iter(vec![
            s_plain("if"),
            s_plain("since"),
            Vector(vec![
                s_plain("$"),
                s_plain("?since"),
            ]),
            Vector(vec![
                s_plain("$"),
            ]),
        ])),
        k_plain("where"),
        s_plain("where"),
    ]);
    assert_eq!(value(test).unwrap(), reply);
}

#[test]
fn test_query_find_title() {
    /*
    // Original query
    :find ?title .
    :in $ ?url
    :where
        [?page :page/url ?url]
        [(get-else $ ?page :page/title \"\") ?title]
    */
    let test = "[
        :find ?title .
        :in $ ?url
        :where
            [?page :page/url ?url]
            [(get-else $ ?page :page/title \"\") ?title]
    ]";

    let reply = Vector(vec![
        k_plain("find"),
        s_plain("?title"),
        s_plain("."),
        k_plain("in"),
        s_plain("$"),
        s_plain("?url"),
        k_plain("where"),
        Vector(vec![
            s_plain("?page"),
            k_ns("page", "url"),
            s_plain("?url"),
        ]),
        Vector(vec![
            List(LinkedList::from_iter(vec![
                s_plain("get-else"),
                s_plain("$"),
                s_plain("?page"),
                k_ns("page", "title"),
                Text("".to_string()),
            ])),
            s_plain("?title"),
        ]),
    ]);
    assert_eq!(value(test).unwrap(), reply);
}

#[test]
fn test_comments() {
    let result = Ok(Value::Integer(0));
    assert_eq!(value("0;"), result);
    assert_eq!(value("0;x"), result);
    assert_eq!(value("0;\n"), result);
    assert_eq!(value("0;x\n"), result);
    assert_eq!(value(";\n0"), result);
    assert_eq!(value(";\r0"), result);
}

#[test]
fn test_whitespace() {
    let result = Ok(Value::Vector(vec![Value::Integer(1)]));
    assert_eq!(value(" [1]"), result);
    assert_eq!(value("[1] "), result);
    assert_eq!(value(" [ 1 ] "), result);
}

#[test]
fn test_inner_whitespace() {
    let result = Ok(Value::Vector(vec![Value::Vector(vec![Value::Integer(1)])]));
    assert_eq!(value("[ [1]]"), result);
    assert_eq!(value("[ [1] ]"), result);
    assert_eq!(value("[[1] ]"), result);
}

#[test]
fn test_commas() {
    let result = Ok(Value::Vector(vec![Value::Integer(1), Value::Integer(2)]));
    assert_eq!(value("[1,2]"), result);
    assert_eq!(value("[1 ,2]"), result);
    assert_eq!(value("[1 , 2]"), result);
    assert_eq!(value("[1 ,2]"), result);
    assert_eq!(value("[ 1,2]"), result);
    assert_eq!(value("[1,2 ]"), result);
}

#[test]
fn test_spurious_commas() {
    let result = Ok(Value::Vector(vec![Value::Integer(3)]));
    assert_eq!(value("[3,]"), result);
    assert_eq!(value("[3 ,]"), result);
    assert_eq!(value("[3 , ]"), result);
    assert_eq!(value("[3, ]"), result);
    assert_eq!(value("[,3]"), result);
    assert_eq!(value("[,,3]"), result);
    assert_eq!(value("[,3,]"), result);
    assert_eq!(value("[3,,]"), result);
}

#[test]
fn test_utils_merge() {
    // Take BTreeMap instances, wrap into Value::Map instances.
    let test = |left: &BTreeMap<Value, Value>, right: &BTreeMap<Value, Value>, expected: &BTreeMap<Value, Value>| {
        let l = Value::Map(left.clone());
        let r = Value::Map(right.clone());
        let result = utils::merge(&l, &r).unwrap();
        let e = Value::Map(expected.clone());
        assert_eq!(result, e);
    };

    let mut left = BTreeMap::new();
    left.insert(Value::Integer(1), Value::Integer(1));
    left.insert(Value::Text("a".into()), Value::Text("a".into()));
    let mut right = BTreeMap::new();
    right.insert(Value::Integer(2), Value::Integer(2));
    right.insert(Value::Text("a".into()), Value::Text("b".into()));

    let mut expected = BTreeMap::new();
    expected.insert(Value::Integer(1), Value::Integer(1));
    expected.insert(Value::Integer(2), Value::Integer(2));
    expected.insert(Value::Text("a".into()), Value::Text("b".into()));

    let mut expected = BTreeMap::new();
    expected.insert(Value::Integer(1), Value::Integer(1));
    expected.insert(Value::Integer(2), Value::Integer(2));
    expected.insert(Value::Text("a".into()), Value::Text("b".into()));
    test(&left, &right, &expected);

    let mut expected = BTreeMap::new();
    expected.insert(Value::Integer(1), Value::Integer(1));
    expected.insert(Value::Integer(2), Value::Integer(2));
    expected.insert(Value::Text("a".into()), Value::Text("a".into()));
    test(&right, &left, &expected);
}

/*
// Handy templates for creating test cases follow:

Text("".to_string()),

Vector(vec![
]),

List(LinkedList::from_iter(vec![
])),

Set(BTreeSet::from_iter(vec![
])),
*/
