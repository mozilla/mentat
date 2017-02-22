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
use std::f64;

use num::bigint::ToBigInt;
use num::traits::{Zero, One};
use ordered_float::OrderedFloat;

use edn::parse::{self, ParseError};
use edn::types::{Value, SpannedValue, Span, ValueAndSpan};
use edn::symbols;
use edn::utils;

// Helper for making wrapped keywords with a namespace.
fn k_ns(ns: &str, name: &str) -> Value {
    Value::NamespacedKeyword(symbols::NamespacedKeyword::new(ns, name))
}

// Helper for making wrapped keywords without a namespace.
fn k_plain(name: &str) -> Value {
    Value::Keyword(symbols::Keyword::new(name))
}

// Helper for making wrapped symbols with a namespace
fn s_ns(ns: &str, name: &str) -> Value {
    Value::NamespacedSymbol(symbols::NamespacedSymbol::new(ns, name))
}

// Helper for making wrapped symbols without a namespace
fn s_plain(name: &str) -> Value {
    Value::PlainSymbol(symbols::PlainSymbol::new(name))
}

// Helpers for parsing strings and converting them into edn::Value.
macro_rules! fn_parse_into_value {
    ($name: ident) => {
        fn $name<'a, T>(src: T) -> Result<Value, ParseError> where T: Into<&'a str> {
            parse::$name(src.into()).map(|x| x.inner.into())
        }
    }
}

// These look exactly like their `parse::foo` counterparts, but
// automatically convert the returned result into Value. Use `parse:foo`
// if you want the original ValueAndSpan instance.
fn_parse_into_value!(nil);
fn_parse_into_value!(nan);
fn_parse_into_value!(infinity);
fn_parse_into_value!(boolean);
fn_parse_into_value!(bigint);
fn_parse_into_value!(octalinteger);
fn_parse_into_value!(hexinteger);
fn_parse_into_value!(basedinteger);
fn_parse_into_value!(integer);
fn_parse_into_value!(float);
fn_parse_into_value!(text);
fn_parse_into_value!(symbol);
fn_parse_into_value!(keyword);
fn_parse_into_value!(list);
fn_parse_into_value!(vector);
fn_parse_into_value!(set);
fn_parse_into_value!(map);
fn_parse_into_value!(value);

#[test]
fn test_nil() {
    use self::Value::*;

    assert_eq!(nil("nil").unwrap(), Nil);

    assert!(nil("true").is_err());
}

#[test]
fn test_span_nil() {
    assert_eq!(parse::nil("nil").unwrap(), ValueAndSpan {
        inner: SpannedValue::Nil,
        span: Span(0, 3)
    });
}

#[test]
fn test_nan() {
    use self::Value::*;

    assert!(nan("#fNaN").is_err());
    assert!(nan("#f;x\nNaN").is_err());

    assert_eq!(nan("#f NaN").unwrap(), Float(OrderedFloat(f64::NAN)));
    assert_eq!(nan("#f\t NaN").unwrap(), Float(OrderedFloat(f64::NAN)));
    assert_eq!(nan("#f,NaN").unwrap(), Float(OrderedFloat(f64::NAN)));

    assert!(nan("true").is_err());
}

#[test]
fn test_span_nan() {
    assert_eq!(parse::nan("#f NaN").unwrap(), ValueAndSpan {
        inner: SpannedValue::Float(OrderedFloat(f64::NAN)),
        span: Span(0, 6)
    });
}

#[test]
fn test_infinity() {
    use self::Value::*;

    assert!(infinity("#f-Infinity").is_err());
    assert!(infinity("#f+Infinity").is_err());

    assert!(infinity("#f;x\n-Infinity").is_err());
    assert!(infinity("#f;x\n+Infinity").is_err());

    assert_eq!(infinity("#f -Infinity").unwrap(), Float(OrderedFloat(f64::NEG_INFINITY)));
    assert_eq!(infinity("#f +Infinity").unwrap(), Float(OrderedFloat(f64::INFINITY)));

    assert_eq!(infinity("#f\t -Infinity").unwrap(), Float(OrderedFloat(f64::NEG_INFINITY)));
    assert_eq!(infinity("#f\t +Infinity").unwrap(), Float(OrderedFloat(f64::INFINITY)));

    assert_eq!(infinity("#f,-Infinity").unwrap(), Float(OrderedFloat(f64::NEG_INFINITY)));
    assert_eq!(infinity("#f,+Infinity").unwrap(), Float(OrderedFloat(f64::INFINITY)));

    assert!(infinity("true").is_err());
}

#[test]
fn test_span_infinity() {
    assert_eq!(parse::infinity("#f -Infinity").unwrap(), ValueAndSpan {
        inner: SpannedValue::Float(OrderedFloat(f64::NEG_INFINITY)),
        span: Span(0, 12)
    });
    assert_eq!(parse::infinity("#f +Infinity").unwrap(), ValueAndSpan {
        inner: SpannedValue::Float(OrderedFloat(f64::INFINITY)),
        span: Span(0, 12)
    });
}

#[test]
fn test_boolean() {
    use self::Value::*;

    assert_eq!(boolean("true").unwrap(), Boolean(true));
    assert_eq!(boolean("false").unwrap(), Boolean(false));

    assert!(boolean("nil").is_err());
}

#[test]
fn test_span_boolean() {
    assert_eq!(parse::boolean("true").unwrap(), ValueAndSpan {
        inner: SpannedValue::Boolean(true),
        span: Span(0, 4)
    });

    assert_eq!(parse::boolean("false").unwrap(), ValueAndSpan {
        inner: SpannedValue::Boolean(false),
        span: Span(0, 5)
    });
}

#[test]
fn test_integer() {
    use self::Value::*;

    assert_eq!(integer("0").unwrap(), Integer(0i64));
    assert_eq!(integer("1").unwrap(), Integer(1i64));
    assert_eq!(integer("999").unwrap(), Integer(999i64));
    assert_eq!(integer("-999").unwrap(), Integer(-999i64));

    assert!(integer("nil").is_err());
}

#[test]
fn test_hexinteger() {
    use self::Value::*;

    assert_eq!(hexinteger("0xabc111").unwrap(), Integer(11256081));
    assert_eq!(hexinteger("0xABCDEF").unwrap(), Integer(11259375));
    assert_eq!(hexinteger("0xabcdef").unwrap(), Integer(11259375));

    assert!(hexinteger("1").is_err());
    assert!(hexinteger("nil").is_err());
    assert!(hexinteger("0xZZZ").is_err());
}

#[test]
fn test_basedinteger() {
    use self::Value::*;

    assert_eq!(basedinteger("2r111").unwrap(), Integer(7));
    assert_eq!(basedinteger("36r1z").unwrap(), Integer(71));
    assert_eq!(basedinteger("36r1Z").unwrap(), Integer(71));
    assert_eq!(basedinteger("12r11").unwrap(), Integer(13));
    assert_eq!(basedinteger("24r10").unwrap(), Integer(24));

    assert!(basedinteger("1").is_err());
    assert!(basedinteger("nil").is_err());
}

#[test]
fn test_octalinteger() {
    use self::Value::*;

    assert_eq!(octalinteger("011").unwrap(), Integer(9));
    assert_eq!(octalinteger("00107").unwrap(), Integer(71));

    assert!(octalinteger("1").is_err());
    assert!(octalinteger("nil").is_err());
}

#[test]
fn test_span_integer() {
    assert_eq!(parse::integer("42").unwrap(), ValueAndSpan {
        inner: SpannedValue::Integer(42),
        span: Span(0, 2)
    });
    assert_eq!(parse::hexinteger("0xabc111").unwrap(), ValueAndSpan {
        inner: SpannedValue::Integer(11256081),
        span: Span(0, 8)
    });
    assert_eq!(parse::basedinteger("2r111").unwrap(), ValueAndSpan {
        inner: SpannedValue::Integer(7),
        span: Span(0, 5)
    });
    assert_eq!(parse::octalinteger("011").unwrap(), ValueAndSpan {
        inner: SpannedValue::Integer(9),
        span: Span(0, 3)
    });
}

#[test]
fn test_bigint() {
    use self::Value::*;

    let max_i64 = i64::max_value().to_bigint().unwrap();
    let bigger = &max_i64 * &max_i64;

    assert_eq!(bigint("0N").unwrap(), BigInteger(Zero::zero()));
    assert_eq!(bigint("1N").unwrap(), BigInteger(One::one()));
    assert_eq!(bigint("9223372036854775807N").unwrap(), BigInteger(max_i64));
    assert_eq!(bigint("85070591730234615847396907784232501249N").unwrap(), BigInteger(bigger));

    assert!(bigint("nil").is_err());
}

#[test]
fn test_span_bigint() {
    let max_i64 = i64::max_value().to_bigint().unwrap();
    let bigger = &max_i64 * &max_i64;

    assert_eq!(parse::bigint("85070591730234615847396907784232501249N").unwrap(), ValueAndSpan {
        inner: SpannedValue::BigInteger(bigger),
        span: Span(0, 39)
    });
}

#[test]
fn test_float() {
    use self::Value::*;

    assert_eq!(float("111.222").unwrap(), Float(OrderedFloat(111.222f64)));
    assert_eq!(float("3e4").unwrap(), Float(OrderedFloat(3e4f64)));
    assert_eq!(float("-55e-66").unwrap(), Float(OrderedFloat(-55e-66f64)));
    assert_eq!(float("77.88e99").unwrap(), Float(OrderedFloat(77.88e99f64)));
    assert_eq!(float("-9.9E-9").unwrap(), Float(OrderedFloat(-9.9E-9f64)));

    assert!(float("42").is_err());
    assert!(float("nil").is_err());
}

#[test]
fn test_span_float() {
    assert_eq!(parse::float("42.0").unwrap(), ValueAndSpan {
        inner: SpannedValue::Float(OrderedFloat(42f64)),
        span: Span(0, 4)
    });
}

#[test]
fn test_text() {
    use self::Value::*;

    assert_eq!(text("\"hello world\"").unwrap(), Text("hello world".to_string()));
    assert_eq!(text("\"\"").unwrap(), Text("".to_string()));

    assert!(text("\"").is_err());
    assert!(text("nil").is_err());
}

#[test]
fn test_span_text() {
    assert_eq!(parse::text("\"hello world\"").unwrap(), ValueAndSpan {
        inner: SpannedValue::Text("hello world".to_string()),
        span: Span(0, 13)
    });
}

#[test]
fn test_symbol() {
    assert_eq!(symbol("$").unwrap(), s_plain("$"));
    assert_eq!(symbol(".").unwrap(), s_plain("."));

    assert_eq!(symbol("hello/world").unwrap(), s_ns("hello", "world"));
    assert_eq!(symbol("foo-bar/baz-boz").unwrap(), s_ns("foo-bar", "baz-boz"));

    assert_eq!(symbol("foo-bar/baz_boz").unwrap(), s_ns("foo-bar", "baz_boz"));
    assert_eq!(symbol("foo_bar/baz-boz").unwrap(), s_ns("foo_bar", "baz-boz"));
    assert_eq!(symbol("foo_bar/baz_boz").unwrap(), s_ns("foo_bar", "baz_boz"));

    assert_eq!(symbol("symbol").unwrap(), s_plain("symbol"));
    assert_eq!(symbol("hello").unwrap(), s_plain("hello"));
    assert_eq!(symbol("foo-bar").unwrap(), s_plain("foo-bar"));
    assert_eq!(symbol("foo_bar").unwrap(), s_plain("foo_bar"));
}

#[test]
fn test_span_symbol() {
    assert_eq!(parse::symbol("hello").unwrap(), ValueAndSpan {
        inner: SpannedValue::from_symbol(None, "hello"),
        span: Span(0, 5)
    });
    assert_eq!(parse::symbol("hello/world").unwrap(), ValueAndSpan {
        inner: SpannedValue::from_symbol("hello", "world"),
        span: Span(0, 11)
    });
}

#[test]
fn test_keyword() {
    assert_eq!(keyword(":hello/world").unwrap(), k_ns("hello", "world"));
    assert_eq!(keyword(":foo-bar/baz-boz").unwrap(), k_ns("foo-bar", "baz-boz"));

    assert_eq!(keyword(":foo-bar/baz_boz").unwrap(), k_ns("foo-bar", "baz_boz"));
    assert_eq!(keyword(":foo_bar/baz-boz").unwrap(), k_ns("foo_bar", "baz-boz"));
    assert_eq!(keyword(":foo_bar/baz_boz").unwrap(), k_ns("foo_bar", "baz_boz"));

    assert_eq!(keyword(":symbol").unwrap(), k_plain("symbol"));
    assert_eq!(keyword(":hello").unwrap(), k_plain("hello"));
    assert_eq!(keyword(":foo-bar").unwrap(), k_plain("foo-bar"));
    assert_eq!(keyword(":foo_bar").unwrap(), k_plain("foo_bar"));

    assert!(keyword(":").is_err());
    assert!(keyword(":foo/").is_err());
}

#[test]
fn test_span_keyword() {
    assert_eq!(parse::keyword(":hello").unwrap(), ValueAndSpan {
        inner: SpannedValue::from_keyword(None, "hello"),
        span: Span(0, 6)
    });
    assert_eq!(parse::keyword(":hello/world").unwrap(), ValueAndSpan {
        inner: SpannedValue::from_keyword("hello", "world"),
        span: Span(0, 12)
    });
}

#[test]
fn test_value() {
    use self::Value::*;

    let max_i64 = i64::max_value().to_bigint().unwrap();
    let bigger = &max_i64 * &max_i64;

    assert_eq!(value("nil").unwrap(), Nil);
    assert_eq!(value("true").unwrap(), Boolean(true));
    assert_eq!(value("1").unwrap(), Integer(1i64));
    assert_eq!(value("0xabc111").unwrap(), Integer(11256081));
    assert_eq!(value("2r111").unwrap(), Integer(7));
    assert_eq!(value("011").unwrap(), Integer(9));
    assert_eq!(value("85070591730234615847396907784232501249N").unwrap(), BigInteger(bigger));
    assert_eq!(value("111.222").unwrap(), Float(OrderedFloat(111.222f64)));
    assert_eq!(value("\"hello world\"").unwrap(), Text("hello world".to_string()));
    assert_eq!(value("$").unwrap(), s_plain("$"));
    assert_eq!(value(".").unwrap(), s_plain("."));
    assert_eq!(value("$symbol").unwrap(), s_plain("$symbol"));
    assert_eq!(value(":hello").unwrap(), k_plain("hello"));
    assert_eq!(value("[1]").unwrap(), Vector(vec![Integer(1)]));
    assert_eq!(value("(1)").unwrap(), List(LinkedList::from_iter(vec![Integer(1)])));
    assert_eq!(value("#{1}").unwrap(), Set(BTreeSet::from_iter(vec![Integer(1)])));
    assert_eq!(value("{1 2}").unwrap(), Map(BTreeMap::from_iter(vec![(Integer(1), Integer(2))])));
}

#[test]
fn test_span_value() {
    let max_i64 = i64::max_value().to_bigint().unwrap();
    let bigger = &max_i64 * &max_i64;

    assert_eq!(parse::value("nil").unwrap(), ValueAndSpan {
        inner: SpannedValue::Nil,
        span: Span(0,3)
    });
    assert_eq!(parse::value("true").unwrap(), ValueAndSpan {
        inner: SpannedValue::Boolean(true),
        span: Span(0,4)
    });
    assert_eq!(parse::value("1").unwrap(), ValueAndSpan {
        inner: SpannedValue::Integer(1i64),
        span: Span(0,1)
    });
    assert_eq!(parse::value("85070591730234615847396907784232501249N").unwrap(), ValueAndSpan {
        inner: SpannedValue::BigInteger(bigger),
        span: Span(0,39)
    });
    assert_eq!(parse::value("111.222").unwrap(), ValueAndSpan {
        inner: SpannedValue::Float(OrderedFloat(111.222f64)),
        span: Span(0,7)
    });
    assert_eq!(parse::value("\"hello world\"").unwrap(), ValueAndSpan {
        inner: SpannedValue::Text("hello world".to_string()),
        span: Span(0,13)
    });
    assert_eq!(parse::value("$").unwrap(), ValueAndSpan {
        inner: SpannedValue::from_symbol(None, "$"),
        span: Span(0,1)
    });
    assert_eq!(parse::value(".").unwrap(), ValueAndSpan {
        inner: SpannedValue::from_symbol(None, "."),
        span: Span(0,1)
    });
    assert_eq!(parse::value("$symbol").unwrap(), ValueAndSpan {
        inner: SpannedValue::from_symbol(None, "$symbol"),
        span: Span(0,7)
    });
    assert_eq!(parse::value(":hello").unwrap(), ValueAndSpan {
        inner: SpannedValue::from_keyword(None, "hello"),
        span: Span(0,6)
    });
    assert_eq!(parse::value("[1]").unwrap(), ValueAndSpan {
        inner: SpannedValue::Vector(vec![
            ValueAndSpan {
                inner: SpannedValue::Integer(1),
                span: Span(1,2)
            }
        ]),
        span: Span(0,3)
    });
    assert_eq!(parse::value("(1)").unwrap(), ValueAndSpan {
        inner: SpannedValue::List(LinkedList::from_iter(vec![
            ValueAndSpan {
                inner: SpannedValue::Integer(1),
                span: Span(1,2)
            }
        ])),
        span: Span(0,3)
    });
    assert_eq!(parse::value("#{1}").unwrap(), ValueAndSpan {
        inner: SpannedValue::Set(BTreeSet::from_iter(vec![
            ValueAndSpan {
                inner: SpannedValue::Integer(1),
                span: Span(2,3)
            }
        ])),
        span: Span(0,4)
    });
    assert_eq!(parse::value("{1 2}").unwrap(), ValueAndSpan {
        inner: SpannedValue::Map(BTreeMap::from_iter(vec![
            (ValueAndSpan {
                inner: SpannedValue::Integer(1),
                span: Span(1,2)
            },
            ValueAndSpan {
                inner: SpannedValue::Integer(2),
                span: Span(3,4)
            })
        ])),
        span: Span(0,5)
    });
}

#[test]
fn test_vector() {
    use self::Value::*;

    let max_i64 = i64::max_value().to_bigint().unwrap();
    let bigger = &max_i64 * &max_i64;

    let test = "[]";
    let value = Vector(vec![
    ]);
    assert_eq!(vector(test).unwrap(), value);

    let test = "[ ]";
    let value = Vector(vec![
    ]);
    assert_eq!(vector(test).unwrap(), value);

    let test = "[1]";
    let value = Vector(vec![
        Integer(1),
    ]);
    assert_eq!(vector(test).unwrap(), value);

    let test = "[ 1 ]";
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
    use self::Value::*;

    let test = "()";
    let value = List(LinkedList::from_iter(vec![
    ]));
    assert_eq!(list(test).unwrap(), value);

    let test = "( )";
    let value = List(LinkedList::from_iter(vec![
    ]));
    assert_eq!(list(test).unwrap(), value);

    let test = "(1)";
    let value = List(LinkedList::from_iter(vec![
        Integer(1),
    ]));
    assert_eq!(list(test).unwrap(), value);

    let test = "( 1 )";
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
    use self::Value::*;

    let test = "#{}";
    let value = Set(BTreeSet::from_iter(vec![
    ]));
    assert_eq!(set(test).unwrap(), value);

    let test = "#{ }";
    let value = Set(BTreeSet::from_iter(vec![
    ]));
    assert_eq!(set(test).unwrap(), value);

    let test = "#{1}";
    let value = Set(BTreeSet::from_iter(vec![
        Integer(1),
    ]));
    assert_eq!(set(test).unwrap(), value);

    let test = "#{ 1 }";
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
    use self::Value::*;

    let test = "{}";
    let value = Map(BTreeMap::from_iter(vec![
    ]));
    assert_eq!(map(test).unwrap(), value);

    let test = "{ }";
    let value = Map(BTreeMap::from_iter(vec![
    ]));
    assert_eq!(map(test).unwrap(), value);

    let test = "{\"a\" 1}";
    let value = Map(BTreeMap::from_iter(vec![
        (Text("a".to_string()), Integer(1)),
    ]));
    assert_eq!(map(test).unwrap(), value);

    let test = "{ \"a\" 1 }";
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

    let test = "{ nil 1 \"b\" 2 }";
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

    let test = "{ nil 1 \"b\" 2 \"a\" 3}";
    let value = Map(BTreeMap::from_iter(vec![
        (Nil, Integer(1)),
        (Text("a".to_string()), Integer(3)),
        (Text("b".to_string()), Integer(2)),
    ]));
    assert_eq!(map(test).unwrap(), value);

    let test = "{:a 1, $b {:b/a nil, :b/b #{nil 5}}, c [1 2], d (3 4)}";
    let value = Map(BTreeMap::from_iter(vec![
        (Keyword(symbols::Keyword::new("a")), Integer(1)),
        (s_plain("$b"), Map(BTreeMap::from_iter(vec![
            (k_ns("b", "a"), Nil),
            (k_ns("b", "b"), Set(BTreeSet::from_iter(vec![
                Nil,
                Integer(5)
            ]))),
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

/// The `test_query_*` functions contain the queries taken from the old Clojure implementation of Mentat.
/// 2 changes have been applied, which should be checked and maybe fixed
/// TODO: Decide if these queries should be placed in a vector wrapper. Is that implied?
/// Secondly, see note in `test_query_starred_pages` on the use of '
#[test]
fn test_query_active_sessions() {
    use self::Value::*;

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
    use self::Value::*;

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
    use self::Value::*;

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
    use self::Value::*;

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
    use self::Value::*;

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
    use self::Value::*;

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
    use self::Value::*;

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
    use self::Value::*;

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

macro_rules! def_test_as_value_type {
    ($value: ident, $method: ident, $is_some: expr, $expected: expr) => {
        if $is_some {
            assert_eq!($value.$method().unwrap(), $expected)
        }
        assert_eq!($value.$method().is_some(), $is_some);
    }
}
macro_rules! def_test_as_type {
    ($value: ident, $method: ident, $is_some: expr, $expected: expr) => {
        if $is_some {
            assert_eq!(*$value.$method().unwrap(), $expected)
        }
        assert_eq!($value.$method().is_some(), $is_some);
    }
}

macro_rules! def_test_into_type {
    ($value: ident, $method: ident, $is_some: expr, $expected: expr) => {
        if $is_some {
            assert_eq!($value.clone().$method().unwrap(), $expected)
        }
        assert_eq!($value.clone().$method().is_some(), $is_some);
    }
}

#[test]
#[cfg_attr(feature = "cargo-clippy", allow(float_cmp))]
fn test_is_and_as_type_helper_functions() {
    let max_i64 = i64::max_value().to_bigint().unwrap();
    let bigger = &max_i64 * &max_i64;

    let values = [
        Value::Nil,
        Value::Boolean(false),
        Value::Integer(1i64),
        Value::BigInteger(bigger),
        Value::Float(OrderedFloat(22.22f64)),
        Value::Text("hello world".to_string()),
        s_plain("$symbol"),
        s_ns("$ns", "$symbol"),
        k_plain("hello"),
        k_ns("hello", "world"),
        Value::Vector(vec![Value::Integer(1)]),
        Value::List(LinkedList::from_iter(vec![])),
        Value::Set(BTreeSet::from_iter(vec![])),
        Value::Map(BTreeMap::from_iter(vec![
            (Value::Text("a".to_string()), Value::Integer(1)),
        ])),
    ];

    for (i, value) in values.iter().enumerate() {
        let is_result = [
            value.is_nil(),
            value.is_boolean(),
            value.is_integer(),
            value.is_big_integer(),
            value.is_float(),
            value.is_text(),
            value.is_symbol(),
            value.is_namespaced_symbol(),
            value.is_keyword(),
            value.is_namespaced_keyword(),
            value.is_vector(),
            value.is_list(),
            value.is_set(),
            value.is_map(),
        ];

        assert_eq!(values.len(), is_result.len());

        for (j, result) in is_result.iter().enumerate() {
            assert_eq!(j == i, *result);
        }

        if i == 0 {
            assert_eq!(value.as_nil().unwrap(), ())
        } else {
            assert!(!value.as_nil().is_some())
        }

        // These return copied values, not references.
        def_test_as_value_type!(value, as_boolean, i == 1, false);
        def_test_as_value_type!(value, as_integer, i == 2, 1i64);
        def_test_as_value_type!(value, as_float, i == 4, 22.22f64);

        def_test_as_type!(value, as_big_integer, i == 3, &max_i64 * &max_i64);
        def_test_as_type!(value, as_ordered_float, i == 4, OrderedFloat(22.22f64));
        def_test_as_type!(value, as_text, i == 5, "hello world".to_string());
        def_test_as_type!(value, as_symbol, i == 6, symbols::PlainSymbol::new("$symbol"));
        def_test_as_type!(value, as_namespaced_symbol, i == 7, symbols::NamespacedSymbol::new("$ns", "$symbol"));
        def_test_as_type!(value, as_keyword, i == 8, symbols::Keyword::new("hello"));
        def_test_as_type!(value, as_namespaced_keyword, i == 9, symbols::NamespacedKeyword::new("hello", "world"));
        def_test_as_type!(value, as_vector, i == 10, vec![Value::Integer(1)]);
        def_test_as_type!(value, as_list, i == 11, LinkedList::from_iter(vec![]));
        def_test_as_type!(value, as_set, i == 12, BTreeSet::from_iter(vec![]));
        def_test_as_type!(value, as_map, i == 13, BTreeMap::from_iter(vec![
            (Value::Text("a".to_string()), Value::Integer(1)),
        ]));

        def_test_into_type!(value, into_boolean, i == 1, false);
        def_test_into_type!(value, into_integer, i == 2, 1i64);
        def_test_into_type!(value, into_float, i == 4, 22.22f64);

        def_test_into_type!(value, into_big_integer, i == 3, &max_i64 * &max_i64);
        def_test_into_type!(value, into_ordered_float, i == 4, OrderedFloat(22.22f64));
        def_test_into_type!(value, into_text, i == 5, "hello world".to_string());
        def_test_into_type!(value, into_symbol, i == 6, symbols::PlainSymbol::new("$symbol"));
        def_test_into_type!(value, into_namespaced_symbol, i == 7, symbols::NamespacedSymbol::new("$ns", "$symbol"));
        def_test_into_type!(value, into_keyword, i == 8, symbols::Keyword::new("hello"));
        def_test_into_type!(value, into_namespaced_keyword, i == 9, symbols::NamespacedKeyword::new("hello", "world"));
        def_test_into_type!(value, into_vector, i == 10, vec![Value::Integer(1)]);
        def_test_into_type!(value, into_list, i == 11, LinkedList::from_iter(vec![]));
        def_test_into_type!(value, into_set, i == 12, BTreeSet::from_iter(vec![]));
        def_test_into_type!(value, into_map, i == 13, BTreeMap::from_iter(vec![
            (Value::Text("a".to_string()), Value::Integer(1)),
        ]));
    }
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
