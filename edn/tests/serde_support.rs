
#![cfg(feature = "serde_support")]

extern crate serde_test;
extern crate serde_json;

extern crate edn;
use edn::symbols::NamespacedKeyword;
use serde_test::{assert_tokens, Token};

#[cfg(feature = "serde_support")]
#[test]
fn test_serialize_keyword() {
    let kw = NamespacedKeyword::new("foo", "bar");
    assert_tokens(&kw, &[
        Token::NewtypeStruct { name: "NamespacedKeyword" },
        Token::Struct { name: "NamespacedName", len: 2 },
        Token::Str("namespace"),
        Token::BorrowedStr("foo"),
        Token::Str("name"),
        Token::BorrowedStr("bar"),
        Token::StructEnd,
    ]);
}


#[cfg(feature = "serde_support")]
#[test]
fn test_deserialize_keyword() {
    let json = r#"{"name": "foo", "namespace": "bar"}"#;
    let kw = serde_json::from_str::<NamespacedKeyword>(json).unwrap();
    assert_eq!(kw.name(), "foo");
    assert_eq!(kw.namespace(), "bar");

    let bad_ns_json = r#"{"name": "foo", "namespace": ""}"#;
    let not_kw = serde_json::from_str::<NamespacedKeyword>(bad_ns_json);
    assert!(not_kw.is_err());

    let bad_ns_json = r#"{"name": "", "namespace": "bar"}"#;
    let not_kw = serde_json::from_str::<NamespacedKeyword>(bad_ns_json);
    assert!(not_kw.is_err());
}



