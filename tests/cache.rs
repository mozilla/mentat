// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate rusqlite;

#[macro_use]
extern crate mentat;
extern crate mentat_core;
extern crate mentat_db;

use std::collections::BTreeSet;

use mentat_core::{
    CachedAttributes,
};

use mentat::{
    Entid,
    HasSchema,
    Queryable,
    Schema,
    Store,
    TypedValue,
};

use mentat_db::cache::{
    SQLiteAttributeCache,
};

fn populate_db() -> Store {
    let mut store = Store::open("").expect("opened");
    {
        let mut write = store.begin_transaction().expect("began transaction");
        let _report = write.transact(r#"[
            {:db/ident       :foo/bar
             :db/valueType   :db.type/long
             :db/cardinality :db.cardinality/one },
            {:db/ident       :foo/baz
             :db/valueType   :db.type/boolean
             :db/cardinality :db.cardinality/one },
            {:db/ident       :foo/bap
             :db/valueType   :db.type/string
             :db/cardinality :db.cardinality/many}]"#).expect("transaction expected to succeed");
        let _report = write.transact(r#"[
            {:db/ident       :item/one
             :foo/bar        100
             :foo/baz        false
             :foo/bap        ["one","two","buckle my shoe"] },
            {:db/ident       :item/two
             :foo/bar        200
             :foo/baz        true
             :foo/bap        ["three", "four", "knock at my door"] }]"#).expect("transaction expected to succeed");
        write.commit().expect("committed");
    }
    store
}

fn assert_value_present_for_attribute(schema: &Schema, attribute_cache: &mut SQLiteAttributeCache, attribute: Entid, entity: Entid, value: TypedValue) {
    let one = attribute_cache.get_value_for_entid(schema, attribute, entity);
    assert!(attribute_cache.get_values_for_entid(schema, attribute, entity).is_none());

    assert_eq!(one, Some(&value));
}

fn assert_values_present_for_attribute(schema: &Schema, attribute_cache: &mut SQLiteAttributeCache, attribute: Entid, entity: Entid, values: Vec<TypedValue>) {
    assert!(attribute_cache.get_value_for_entid(schema, attribute, entity).is_none());
    let actual: BTreeSet<TypedValue> = attribute_cache.get_values_for_entid(schema, attribute, entity)
                                                      .expect("Non-None")
                                                      .clone()
                                                      .into_iter()
                                                      .collect();
    let expected: BTreeSet<TypedValue> = values.into_iter().collect();

    assert_eq!(actual, expected);
}

#[test]
fn test_add_to_cache() {
    let mut store = populate_db();
    let schema = &store.conn().current_schema();
    let mut attribute_cache = SQLiteAttributeCache::default();
    let kw = kw!(:foo/bar);
    let attr: Entid = schema.get_entid(&kw).expect("Expected entid for attribute").into();

    {
        assert!(attribute_cache.value_pairs(schema, attr).is_none());
    }

    attribute_cache.register(&schema, &store.sqlite_mut(), attr).expect("No errors on add to cache");
    {
        let cached_values = attribute_cache.value_pairs(schema, attr).expect("non-None");
        assert!(!cached_values.is_empty());
        let flattened: BTreeSet<TypedValue> = cached_values.values().cloned().filter_map(|x| x).collect();
        let expected: BTreeSet<TypedValue> = vec![TypedValue::Long(100), TypedValue::Long(200)].into_iter().collect();
        assert_eq!(flattened, expected);
    }
}

#[test]
fn test_add_attribute_already_in_cache() {
    let mut store = populate_db();
    let schema = store.conn().current_schema();

    let kw = kw!(:foo/bar);
    let attr: Entid = schema.get_entid(&kw).expect("Expected entid for attribute").into();
    let mut attribute_cache = SQLiteAttributeCache::default();

    let one = schema.get_entid(&kw!(:item/one)).expect("one");
    let two = schema.get_entid(&kw!(:item/two)).expect("two");
    attribute_cache.register(&schema, &mut store.sqlite_mut(), attr).expect("No errors on add to cache");
    assert_value_present_for_attribute(&schema, &mut attribute_cache, attr.into(), one.into(), TypedValue::Long(100));
    attribute_cache.register(&schema, &mut store.sqlite_mut(), attr).expect("No errors on add to cache");
    assert_value_present_for_attribute(&schema, &mut attribute_cache, attr.into(), two.into(), TypedValue::Long(200));
}

#[test]
fn test_remove_from_cache() {
    let mut store = populate_db();
    let schema = store.conn().current_schema();

    let kwr = kw!(:foo/bar);
    let entidr: Entid = schema.get_entid(&kwr).expect("Expected entid for attribute").into();
    let kwz = kw!(:foo/baz);
    let entidz: Entid = schema.get_entid(&kwz).expect("Expected entid for attribute").into();
    let kwp = kw!(:foo/bap);
    let entidp: Entid = schema.get_entid(&kwp).expect("Expected entid for attribute").into();

    let mut attribute_cache = SQLiteAttributeCache::default();

    let one = schema.get_entid(&kw!(:item/one)).expect("one");
    let two = schema.get_entid(&kw!(:item/two)).expect("two");
    assert!(attribute_cache.get_value_for_entid(&schema, entidz, one.into()).is_none());
    assert!(attribute_cache.get_values_for_entid(&schema, entidz, one.into()).is_none());
    assert!(attribute_cache.get_value_for_entid(&schema, entidz, two.into()).is_none());
    assert!(attribute_cache.get_values_for_entid(&schema, entidz, two.into()).is_none());
    assert!(attribute_cache.get_value_for_entid(&schema, entidp, one.into()).is_none());
    assert!(attribute_cache.get_values_for_entid(&schema, entidp, one.into()).is_none());

    attribute_cache.register(&schema, &mut store.sqlite_mut(), entidr).expect("No errors on add to cache");
    assert_value_present_for_attribute(&schema, &mut attribute_cache, entidr, one.into(), TypedValue::Long(100));
    assert_value_present_for_attribute(&schema, &mut attribute_cache, entidr, two.into(), TypedValue::Long(200));
    attribute_cache.register(&schema, &mut store.sqlite_mut(), entidz).expect("No errors on add to cache");
    assert_value_present_for_attribute(&schema, &mut attribute_cache, entidz, one.into(), TypedValue::Boolean(false));
    assert_value_present_for_attribute(&schema, &mut attribute_cache, entidz, one.into(), TypedValue::Boolean(false));
    attribute_cache.register(&schema, &mut store.sqlite_mut(), entidp).expect("No errors on add to cache");
    assert_values_present_for_attribute(&schema, &mut attribute_cache, entidp, one.into(),
        vec![TypedValue::typed_string("buckle my shoe"),
             TypedValue::typed_string("one"),
             TypedValue::typed_string("two")]);
    assert_values_present_for_attribute(&schema, &mut attribute_cache, entidp, two.into(),
        vec![TypedValue::typed_string("knock at my door"),
             TypedValue::typed_string("three"),
             TypedValue::typed_string("four")]);

    // test that we can remove an item from cache
    attribute_cache.unregister(entidz);
    assert!(!attribute_cache.is_attribute_cached_forward(entidz.into()));
    assert!(attribute_cache.get_value_for_entid(&schema, entidz, one.into()).is_none());
    assert!(attribute_cache.get_values_for_entid(&schema, entidz, one.into()).is_none());
    assert!(attribute_cache.get_value_for_entid(&schema, entidz, two.into()).is_none());
    assert!(attribute_cache.get_values_for_entid(&schema, entidz, two.into()).is_none());
}

#[test]
fn test_remove_attribute_not_in_cache() {
    let store = populate_db();
    let mut attribute_cache = SQLiteAttributeCache::default();

    let schema = store.conn().current_schema();
    let kw = kw!(:foo/baz);
    let entid = schema.get_entid(&kw).expect("Expected entid for attribute").0;
    attribute_cache.unregister(entid);
    assert!(!attribute_cache.is_attribute_cached_forward(entid));
}

#[test]
fn test_fetch_attribute_value_for_entid() {
    let mut store = populate_db();
    let schema = store.conn().current_schema();

    let entities = store.q_once(r#"[:find ?e . :where [?e :foo/bar 100]]"#, None).expect("Expected query to work").into_scalar().expect("expected scalar results");
    let entid = match entities {
        Some(TypedValue::Ref(entid)) => entid,
        x => panic!("expected Some(Ref), got {:?}", x),
    };

    let kwr = kw!(:foo/bar);
    let attr_entid = schema.get_entid(&kwr).expect("Expected entid for attribute").0;

    let mut attribute_cache = SQLiteAttributeCache::default();

    attribute_cache.register(&schema, &mut store.sqlite_mut(), attr_entid).expect("No errors on add to cache");
    let val = attribute_cache.get_value_for_entid(&schema, attr_entid, entid).expect("Expected value");
    assert_eq!(*val, TypedValue::Long(100));
}

#[test]
fn test_fetch_attribute_values_for_entid() {
    let mut store = populate_db();
    let schema = store.conn().current_schema();

    let entities = store.q_once(r#"[:find ?e . :where [?e :foo/bar 100]]"#, None).expect("Expected query to work").into_scalar().expect("expected scalar results");
    let entid = match entities {
        Some(TypedValue::Ref(entid)) => entid,
        x => panic!("expected Some(Ref), got {:?}", x),
    };

    let kwp = kw!(:foo/bap);
    let attr_entid = schema.get_entid(&kwp).expect("Expected entid for attribute").0;

    let mut attribute_cache = SQLiteAttributeCache::default();

    attribute_cache.register(&schema, &mut store.sqlite_mut(), attr_entid).expect("No errors on add to cache");
    let val = attribute_cache.get_values_for_entid(&schema, attr_entid, entid).expect("Expected value");
    assert_eq!(*val, vec![TypedValue::typed_string("buckle my shoe"),
                          TypedValue::typed_string("one"),
                          TypedValue::typed_string("two")]);
}
