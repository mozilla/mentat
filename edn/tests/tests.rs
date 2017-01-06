// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate datomish_edn;
extern crate num;
extern crate ordered_float;

use std::collections::{BTreeSet, BTreeMap, LinkedList};
use std::iter::FromIterator;
use num::bigint::ToBigInt;
use num::traits::{Zero, One};
use ordered_float::OrderedFloat;
use datomish_edn::types::Value::*;
use datomish_edn::parse::*;

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
    assert_eq!(symbol("$").unwrap(), Symbol("$".to_string()));
    assert_eq!(symbol(".").unwrap(), Symbol(".".to_string()));
    assert_eq!(symbol("r_r").unwrap(), Symbol("r_r".to_string()));
    assert_eq!(symbol("$symbol").unwrap(), Symbol("$symbol".to_string()));
    assert_eq!(symbol("hello").unwrap(), Symbol("hello".to_string()));
}

#[test]
fn test_keyword() {
    assert_eq!(keyword(":hello/world").unwrap(), Keyword(":hello/world".to_string()));
    assert_eq!(keyword(":symbol").unwrap(), Keyword(":symbol".to_string()));
    assert_eq!(keyword(":hello").unwrap(), Keyword(":hello".to_string()));
}

#[test]
fn test_value() {
    let max_i64 = i64::max_value().to_bigint().unwrap();
    let bigger = &max_i64 * &max_i64;

    assert_eq!(value("nil").unwrap(), Nil);
    assert_eq!(value("true").unwrap(), Boolean(true));
    assert_eq!(value("1").unwrap(), Integer(1i64));
    assert_eq!(value("\"hello world\"").unwrap(), Text("hello world".to_string()));
    assert_eq!(value("$").unwrap(), Symbol("$".to_string()));
    assert_eq!(value(".").unwrap(), Symbol(".".to_string()));
    assert_eq!(value("$symbol").unwrap(), Symbol("$symbol".to_string()));
    assert_eq!(value(":hello").unwrap(), Keyword(":hello".to_string()));
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
    let value = Map(BTreeMap::from_iter(vec![
        (Keyword(":a".to_string()), Integer(1)),
        (Symbol("$b".to_string()), Map(BTreeMap::from_iter(vec![
            (Keyword(":b/a".to_string()), Nil),
            (Keyword(":b/b".to_string()), Set(BTreeSet::from_iter(vec![
                Nil,
                Integer(5),
            ]))),
        ]))),
        (Symbol("c".to_string()), Vector(vec![
            Integer(1),
            Integer(2),
        ])),
        (Symbol("d".to_string()), List(LinkedList::from_iter(vec![
            Integer(3),
            Integer(4),
        ]))),
    ]));
    assert_eq!(map(test).unwrap(), value);

    assert!(map("#{").is_err());
    assert!(map("}").is_err());
    assert!(map("1}").is_err());
    assert!(map("#{1 #{2 nil} \"hi\"").is_err());
}

/// The test_query_* functions contain the queries taken from the old clojure datomish.
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
        Keyword(":find".to_string()),
        Symbol("?id".to_string()),
        Symbol("?reason".to_string()),
        Symbol("?ts".to_string()),
        Keyword(":in".to_string()),
        Symbol("$".to_string()),
        Keyword(":where".to_string()),
        Vector(vec![
            Symbol("?id".to_string()),
            Keyword(":session/startReason".to_string()),
            Symbol("?reason".to_string()),
            Symbol("?tx".to_string()),
        ]),
        Vector(vec![
            Symbol("?tx".to_string()),
            Keyword(":db/txInstant".to_string()),
            Symbol("?ts".to_string()),
        ]),
        List(LinkedList::from_iter(vec![
            Symbol("not-join".to_string()),
            Vector(vec![
                Symbol("?id".to_string()),
            ]),
            Vector(vec![
                Symbol("?id".to_string()),
                Keyword(":session/endReason".to_string()),
                Symbol("_".to_string()),
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
        Keyword(":find".to_string()),
        Symbol("?id".to_string()),
        Symbol("?endReason".to_string()),
        Symbol("?ts".to_string()),
        Keyword(":in".to_string()),
        Symbol("$".to_string()),
        Keyword(":where".to_string()),
        Vector(vec![
            Symbol("?id".to_string()),
            Keyword(":session/endReason".to_string()),
            Symbol("?endReason".to_string()),
            Symbol("?tx".to_string()),
        ]),
        Vector(vec![
            Symbol("?tx".to_string()),
            Keyword(":db/txInstant".to_string()),
            Symbol("?ts".to_string()),
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
        Keyword(":find".to_string()),
        Vector(vec![
            Symbol("?url".to_string()),
            Symbol("?title".to_string()),
            Symbol("?starredOn".to_string()),
        ]),
        Keyword(":in".to_string()),
        List(LinkedList::from_iter(vec![
            Symbol("if".to_string()),
            Symbol("since".to_string()),
            Vector(vec![
                Symbol("$".to_string()),
                Symbol("?since".to_string()),
            ]),
            Vector(vec![
                Symbol("$".to_string()),
            ]),
        ])),
        Keyword(":where".to_string()),
        Symbol("where".to_string()),
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
        Keyword(":find".to_string()),
        Symbol("?page".to_string()),
        Symbol("?url".to_string()),
        Symbol("?title".to_string()),
        Symbol("?excerpt".to_string()),
        Keyword(":in".to_string()),
        Symbol("$".to_string()),
        Keyword(":where".to_string()),
        Vector(vec![
            Symbol("?save".to_string()),
            Keyword(":save/page".to_string()),
            Symbol("?page".to_string()),
        ]),
        Vector(vec![
            Symbol("?save".to_string()),
            Keyword(":save/savedAt".to_string()),
            Symbol("?instant".to_string()),
        ]),
        Vector(vec![
            Symbol("?page".to_string()),
            Keyword(":page/url".to_string()),
            Symbol("?url".to_string()),
        ]),
        Vector(vec![
            List(LinkedList::from_iter(vec![
                Symbol("get-else".to_string()),
                Symbol("$".to_string()),
                Symbol("?save".to_string()),
                Keyword(":save/title".to_string()),
                Text("".to_string()),
            ])),
            Symbol("?title".to_string()),
        ]),
        Vector(vec![
            List(LinkedList::from_iter(vec![
                Symbol("get-else".to_string()),
                Symbol("$".to_string()),
                Symbol("?save".to_string()),
                Keyword(":save/excerpt".to_string()),
                Text("".to_string()),
            ])),
            Symbol("?excerpt".to_string()),
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
        Keyword(":find".to_string()),
        Vector(vec![
            Symbol("?url".to_string()),
            Symbol("?title".to_string()),
        ]),
        Keyword(":in".to_string()),
        Vector(vec![
            Symbol("$".to_string()),
        ]),
        Keyword(":where".to_string()),
        Vector(vec![
            Vector(vec![
                List(LinkedList::from_iter(vec![
                    Symbol("list".to_string()),
                    Symbol("fulltext".to_string()),
                    Symbol("$".to_string()),
                    Set(BTreeSet::from_iter(vec![
                        Keyword(":page/url".to_string()),
                        Keyword(":page/title".to_string()),
                    ])),
                    Symbol("string".to_string()),
                ])),
                Vector(vec![
                    Vector(vec![
                        Symbol("?page".to_string()),
                    ]),
                ]),
            ]),
            Vector(vec![
                List(LinkedList::from_iter(vec![
                    Symbol("get-else".to_string()),
                    Symbol("$".to_string()),
                    Symbol("?page".to_string()),
                    Keyword(":page/url".to_string()),
                    Text("".to_string()),
                ])),
                Symbol("?url".to_string()),
            ]),
            Vector(vec![
                List(LinkedList::from_iter(vec![
                    Symbol("get-else".to_string()),
                    Symbol("$".to_string()),
                    Symbol("?page".to_string()),
                    Keyword(":page/title".to_string()),
                    Text("".to_string()),
                ])),
                Symbol("?title".to_string()),
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
        Keyword(":find".to_string()),
        Vector(vec![
            Symbol("?url".to_string()),
            Symbol("?title".to_string()),
            Symbol("?excerpt".to_string()),
        ]),
        Keyword(":in".to_string()),
        Vector(vec![
            Symbol("$".to_string()),
        ]),
        Keyword(":where".to_string()),
        Vector(vec![
            Vector(vec![
                List(LinkedList::from_iter(vec![
                    Symbol("list".to_string()),
                    Symbol("fulltext".to_string()),
                    Symbol("$".to_string()),
                    Set(BTreeSet::from_iter(vec![
                        Keyword(":save/title".to_string()),
                        Keyword(":save/excerpt".to_string()),
                        Keyword(":save/content".to_string()),
                    ])),
                    Symbol("string".to_string()),
                ])),
                Vector(vec![
                    Vector(vec![
                        Symbol("?save".to_string()),
                    ]),
                ]),
            ]),
            Vector(vec![
                Symbol("?save".to_string()),
                Keyword(":save/page".to_string()),
                Symbol("?page".to_string()),
            ]),
            Vector(vec![
                Symbol("?page".to_string()),
                Keyword(":page/url".to_string()),
                Symbol("?url".to_string()),
            ]),
            Vector(vec![
                List(LinkedList::from_iter(vec![
                    Symbol("get-else".to_string()),
                    Symbol("$".to_string()),
                    Symbol("?save".to_string()),
                    Keyword(":save/title".to_string()),
                    Text("".to_string()),
                ])),
                Symbol("?title".to_string()),
            ]),
            Vector(vec![
                List(LinkedList::from_iter(vec![
                    Symbol("get-else".to_string()),
                    Symbol("$".to_string()),
                    Symbol("?save".to_string()),
                    Keyword(":save/excerpt".to_string()),
                    Text("".to_string()),
                ])),
                Symbol("?excerpt".to_string()),
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
        Keyword(":find".to_string()),
        Vector(vec![
            Symbol("?url".to_string()),
            Symbol("?title".to_string()),
            List(LinkedList::from_iter(vec![
                Symbol("max".to_string()),
                Symbol("?time".to_string()),
            ])),
        ]),
        Keyword(":in".to_string()),
        List(LinkedList::from_iter(vec![
            Symbol("if".to_string()),
            Symbol("since".to_string()),
            Vector(vec![
                Symbol("$".to_string()),
                Symbol("?since".to_string()),
            ]),
            Vector(vec![
                Symbol("$".to_string()),
            ]),
        ])),
        Keyword(":where".to_string()),
        Symbol("where".to_string()),
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
        Keyword(":find".to_string()),
        Symbol("?title".to_string()),
        Symbol(".".to_string()),
        Keyword(":in".to_string()),
        Symbol("$".to_string()),
        Symbol("?url".to_string()),
        Keyword(":where".to_string()),
        Vector(vec![
            Symbol("?page".to_string()),
            Keyword(":page/url".to_string()),
            Symbol("?url".to_string()),
        ]),
        Vector(vec![
            List(LinkedList::from_iter(vec![
                Symbol("get-else".to_string()),
                Symbol("$".to_string()),
                Symbol("?page".to_string()),
                Keyword(":page/title".to_string()),
                Text("".to_string()),
            ])),
            Symbol("?title".to_string()),
        ]),
    ]);
    assert_eq!(value(test).unwrap(), reply);
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
