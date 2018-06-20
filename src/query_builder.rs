// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![macro_use]
use std::collections::{
    BTreeMap,
};

use mentat_core::{
    DateTime,
    Entid,
    Keyword,
    Binding,
    TypedValue,
    Utc,
    ValueType,
};

use ::{
    HasSchema,
    Queryable,
    QueryInputs,
    QueryOutput,
    RelResult,
    Store,
    Variable,
};

use errors::{
    MentatError,
    Result,
};

pub struct QueryBuilder<'a> {
    query: String,
    values: BTreeMap<Variable, TypedValue>,
    types: BTreeMap<Variable, ValueType>,
    store: &'a mut Store,
}

impl<'a> QueryBuilder<'a> {
    pub fn new<T>(store: &'a mut Store, query: T) -> QueryBuilder where T: Into<String> {
        QueryBuilder { query: query.into(), values: BTreeMap::new(), types: BTreeMap::new(), store }
    }

    pub fn bind_value<T>(&mut self, var: &str, value: T) -> &mut Self where T: Into<TypedValue> {
        self.values.insert(Variable::from_valid_name(var), value.into());
        self
    }

    pub fn bind_ref_from_kw(&mut self, var: &str, value: Keyword) -> Result<&mut Self> {
        let entid = self.store.conn().current_schema().get_entid(&value).ok_or(MentatError::UnknownAttribute(value.to_string()))?;
        self.values.insert(Variable::from_valid_name(var), TypedValue::Ref(entid.into()));
        Ok(self)
    }

    pub fn bind_ref<T>(&mut self, var: &str, value: T) -> &mut Self where T: Into<Entid> {
       self.values.insert(Variable::from_valid_name(var), TypedValue::Ref(value.into()));
       self
    }

    pub fn bind_long(&mut self, var: &str, value: i64) -> &mut Self {
       self.values.insert(Variable::from_valid_name(var), TypedValue::Long(value));
       self
    }

    pub fn bind_instant(&mut self, var: &str, value: i64) -> &mut Self {
       self.values.insert(Variable::from_valid_name(var), TypedValue::instant(value));

       self
    }

    pub fn bind_date_time(&mut self, var: &str, value: DateTime<Utc>) -> &mut Self {
       self.values.insert(Variable::from_valid_name(var), TypedValue::Instant(value));
       self
    }

    pub fn bind_type(&mut self, var: &str, value_type: ValueType) -> &mut Self {
        self.types.insert(Variable::from_valid_name(var), value_type);
        self
    }

    pub fn execute(&mut self) -> Result<QueryOutput> {
        let values = ::std::mem::replace(&mut self.values, Default::default());
        let types = ::std::mem::replace(&mut self.types, Default::default());
        let query_inputs = QueryInputs::new(types, values)?;
        let read = self.store.begin_read()?;
        read.q_once(&self.query, query_inputs)
    }

    pub fn execute_scalar(&mut self) -> Result<Option<Binding>> {
        let results = self.execute()?;
        results.into_scalar().map_err(|e| e.into())
    }

    pub fn execute_coll(&mut self) -> Result<Vec<Binding>> {
        let results = self.execute()?;
        results.into_coll().map_err(|e| e.into())
    }

    pub fn execute_tuple(&mut self) -> Result<Option<Vec<Binding>>> {
        let results = self.execute()?;
        results.into_tuple().map_err(|e| e.into())
    }

    pub fn execute_rel(&mut self) -> Result<RelResult<Binding>> {
        let results = self.execute()?;
        results.into_rel().map_err(|e| e.into())
    }
}

#[cfg(test)]
mod test {
    use super::{
        QueryBuilder,
        TypedValue,
        Store,
    };

    #[test]
    fn test_scalar_query() {
        let mut store = Store::open("").expect("store connection");
        store.transact(r#"[
            [:db/add "s" :db/ident :foo/boolean]
            [:db/add "s" :db/valueType :db.type/boolean]
            [:db/add "s" :db/cardinality :db.cardinality/one]
        ]"#).expect("successful transaction");

        let report = store.transact(r#"[
            [:db/add "u" :foo/boolean true]
            [:db/add "p" :foo/boolean false]
        ]"#).expect("successful transaction");

        let yes = report.tempids.get("u").expect("found it").clone();

        let entid = QueryBuilder::new(&mut store, r#"[:find ?x .
                                                      :in ?v
                                                      :where [?x :foo/boolean ?v]]"#)
                              .bind_value("?v", true)
                              .execute_scalar().expect("ScalarResult")
                              .map_or(None, |t| t.into_entid());
        assert_eq!(entid, Some(yes));
    }

    #[test]
    fn test_coll_query() {
        let mut store = Store::open("").expect("store connection");
        store.transact(r#"[
            [:db/add "s" :db/ident :foo/boolean]
            [:db/add "s" :db/valueType :db.type/boolean]
            [:db/add "s" :db/cardinality :db.cardinality/one]
            [:db/add "t" :db/ident :foo/long]
            [:db/add "t" :db/valueType :db.type/long]
            [:db/add "t" :db/cardinality :db.cardinality/one]
        ]"#).expect("successful transaction");

        let report = store.transact(r#"[
            [:db/add "l" :foo/boolean true]
            [:db/add "l" :foo/long 25]
            [:db/add "m" :foo/boolean false]
            [:db/add "m" :foo/long 26]
            [:db/add "n" :foo/boolean true]
            [:db/add "n" :foo/long 27]
            [:db/add "p" :foo/boolean false]
            [:db/add "p" :foo/long 24]
            [:db/add "u" :foo/boolean true]
            [:db/add "u" :foo/long 23]
        ]"#).expect("successful transaction");

        let u_yes = report.tempids.get("u").expect("found it").clone();
        let l_yes = report.tempids.get("l").expect("found it").clone();
        let n_yes = report.tempids.get("n").expect("found it").clone();

        let entids: Vec<i64> = QueryBuilder::new(&mut store, r#"[:find [?x ...]
                                                                 :in ?v
                                                                 :where [?x :foo/boolean ?v]]"#)
                              .bind_value("?v", true)
                              .execute_coll().expect("CollResult")
                              .into_iter()
                              .map(|v| v.into_entid().expect("val"))
                              .collect();

        assert_eq!(entids, vec![l_yes, n_yes, u_yes]);
    }

    #[test]
    fn test_coll_query_by_row() {
        let mut store = Store::open("").expect("store connection");
        store.transact(r#"[
            [:db/add "s" :db/ident :foo/boolean]
            [:db/add "s" :db/valueType :db.type/boolean]
            [:db/add "s" :db/cardinality :db.cardinality/one]
            [:db/add "t" :db/ident :foo/long]
            [:db/add "t" :db/valueType :db.type/long]
            [:db/add "t" :db/cardinality :db.cardinality/one]
        ]"#).expect("successful transaction");

        let report = store.transact(r#"[
            [:db/add "l" :foo/boolean true]
            [:db/add "l" :foo/long 25]
            [:db/add "m" :foo/boolean false]
            [:db/add "m" :foo/long 26]
            [:db/add "n" :foo/boolean true]
            [:db/add "n" :foo/long 27]
            [:db/add "p" :foo/boolean false]
            [:db/add "p" :foo/long 24]
            [:db/add "u" :foo/boolean true]
            [:db/add "u" :foo/long 23]
        ]"#).expect("successful transaction");

        let n_yes = report.tempids.get("n").expect("found it").clone();

        let results = QueryBuilder::new(&mut store, r#"[:find [?x ...]
                                                        :in ?v
                                                        :where [?x :foo/boolean ?v]]"#)
                              .bind_value("?v", true)
                              .execute_coll().expect("CollResult");
        let entid = results.get(1).map_or(None, |t| t.to_owned().into_entid()).expect("entid");

        assert_eq!(entid, n_yes);
    }

    #[test]
    fn test_tuple_query_result_by_column() {
        let mut store = Store::open("").expect("store connection");
        store.transact(r#"[
            [:db/add "s" :db/ident :foo/boolean]
            [:db/add "s" :db/valueType :db.type/boolean]
            [:db/add "s" :db/cardinality :db.cardinality/one]
            [:db/add "t" :db/ident :foo/long]
            [:db/add "t" :db/valueType :db.type/long]
            [:db/add "t" :db/cardinality :db.cardinality/one]
        ]"#).expect("successful transaction");

        let report = store.transact(r#"[
            [:db/add "l" :foo/boolean true]
            [:db/add "l" :foo/long 25]
            [:db/add "m" :foo/boolean false]
            [:db/add "m" :foo/long 26]
            [:db/add "n" :foo/boolean true]
            [:db/add "n" :foo/long 27]
            [:db/add "p" :foo/boolean false]
            [:db/add "p" :foo/long 24]
            [:db/add "u" :foo/boolean true]
            [:db/add "u" :foo/long 23]
        ]"#).expect("successful transaction");

        let n_yes = report.tempids.get("n").expect("found it").clone();

        let results = QueryBuilder::new(&mut store, r#"[:find [?x, ?i]
                                                        :in ?v ?i
                                                        :where [?x :foo/boolean ?v]
                                                               [?x :foo/long ?i]]"#)
                              .bind_value("?v", true)
                              .bind_long("?i", 27)
                              .execute_tuple().expect("TupleResult").expect("Vec<TypedValue>");
        let entid = results.get(0).map_or(None, |t| t.to_owned().into_entid()).expect("entid");
        let long_val = results.get(1).map_or(None, |t| t.to_owned().into_long()).expect("long");

        assert_eq!(entid, n_yes);
        assert_eq!(long_val, 27);
    }

    #[test]
    fn test_tuple_query_result_by_iter() {
        let mut store = Store::open("").expect("store connection");
        store.transact(r#"[
            [:db/add "s" :db/ident :foo/boolean]
            [:db/add "s" :db/valueType :db.type/boolean]
            [:db/add "s" :db/cardinality :db.cardinality/one]
            [:db/add "t" :db/ident :foo/long]
            [:db/add "t" :db/valueType :db.type/long]
            [:db/add "t" :db/cardinality :db.cardinality/one]
        ]"#).expect("successful transaction");

        let report = store.transact(r#"[
            [:db/add "l" :foo/boolean true]
            [:db/add "l" :foo/long 25]
            [:db/add "m" :foo/boolean false]
            [:db/add "m" :foo/long 26]
            [:db/add "n" :foo/boolean true]
            [:db/add "n" :foo/long 27]
            [:db/add "p" :foo/boolean false]
            [:db/add "p" :foo/long 24]
            [:db/add "u" :foo/boolean true]
            [:db/add "u" :foo/long 23]
        ]"#).expect("successful transaction");

        let n_yes = report.tempids.get("n").expect("found it").clone();

        let results: Vec<_> = QueryBuilder::new(&mut store, r#"[:find [?x, ?i]
                                                                :in ?v ?i
                                                                :where [?x :foo/boolean ?v]
                                                                       [?x :foo/long ?i]]"#)
                              .bind_value("?v", true)
                              .bind_long("?i", 27)
                              .execute_tuple().expect("TupleResult").unwrap_or(vec![]);
        let entid = TypedValue::Ref(n_yes.clone()).into();
        let long_val = TypedValue::Long(27).into();

        assert_eq!(results, vec![entid, long_val]);
    }

    #[test]
    fn test_rel_query_result() {
        let mut store = Store::open("").expect("store connection");
        store.transact(r#"[
            [:db/add "s" :db/ident :foo/boolean]
            [:db/add "s" :db/valueType :db.type/boolean]
            [:db/add "s" :db/cardinality :db.cardinality/one]
            [:db/add "t" :db/ident :foo/long]
            [:db/add "t" :db/valueType :db.type/long]
            [:db/add "t" :db/cardinality :db.cardinality/one]
        ]"#).expect("successful transaction");

        let report = store.transact(r#"[
            [:db/add "l" :foo/boolean true]
            [:db/add "l" :foo/long 25]
            [:db/add "m" :foo/boolean false]
            [:db/add "m" :foo/long 26]
            [:db/add "n" :foo/boolean true]
            [:db/add "n" :foo/long 27]
        ]"#).expect("successful transaction");

        let l_yes = report.tempids.get("l").expect("found it").clone();
        let m_yes = report.tempids.get("m").expect("found it").clone();
        let n_yes = report.tempids.get("n").expect("found it").clone();

        #[derive(Debug, PartialEq)]
        struct Res {
            entid: i64,
            boolean: bool,
            long_val: i64,
        };

        let mut results: Vec<Res> = QueryBuilder::new(&mut store, r#"[:find ?x ?v ?i
                                                                      :where [?x :foo/boolean ?v]
                                                                             [?x :foo/long ?i]]"#)
                              .execute_rel().expect("RelResult")
                              .into_iter()
                              .map(|row| {
                                  Res {
                                      entid: row.get(0).map_or(None, |t| t.to_owned().into_entid()).expect("entid"),
                                      boolean: row.get(1).map_or(None, |t| t.to_owned().into_boolean()).expect("boolean"),
                                      long_val: row.get(2).map_or(None, |t| t.to_owned().into_long()).expect("long"),
                                  }
                              })
                              .collect();

        let res1 = results.pop().expect("res");
        assert_eq!(res1, Res { entid: n_yes, boolean: true, long_val: 27 });
        let res2 = results.pop().expect("res");
        assert_eq!(res2, Res { entid: m_yes, boolean: false, long_val: 26 });
        let res3 = results.pop().expect("res");
        assert_eq!(res3, Res { entid: l_yes, boolean: true, long_val: 25 });
        assert_eq!(results.pop(), None);
    }

    #[test]
    fn test_bind_ref() {
        let mut store = Store::open("").expect("store connection");
        store.transact(r#"[
            [:db/add "s" :db/ident :foo/boolean]
            [:db/add "s" :db/valueType :db.type/boolean]
            [:db/add "s" :db/cardinality :db.cardinality/one]
            [:db/add "t" :db/ident :foo/long]
            [:db/add "t" :db/valueType :db.type/long]
            [:db/add "t" :db/cardinality :db.cardinality/one]
        ]"#).expect("successful transaction");

        let report = store.transact(r#"[
            [:db/add "l" :foo/boolean true]
            [:db/add "l" :foo/long 25]
            [:db/add "m" :foo/boolean false]
            [:db/add "m" :foo/long 26]
            [:db/add "n" :foo/boolean true]
            [:db/add "n" :foo/long 27]
        ]"#).expect("successful transaction");

        let l_yes = report.tempids.get("l").expect("found it").clone();

        let results = QueryBuilder::new(&mut store, r#"[:find [?v ?i]
                                                        :in ?x
                                                        :where [?x :foo/boolean ?v]
                                                               [?x :foo/long ?i]]"#)
                              .bind_ref("?x", l_yes)
                              .execute_tuple().expect("TupleResult")
                              .unwrap_or(vec![]);
        assert_eq!(results.get(0).map_or(None, |t| t.to_owned().into_boolean()).expect("boolean"), true);
        assert_eq!(results.get(1).map_or(None, |t| t.to_owned().into_long()).expect("long"), 25);
    }
}
