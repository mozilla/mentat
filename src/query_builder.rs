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
use std::rc::Rc;

use indexmap::{
    IndexMap
};

use indexmap;

use mentat_core::{
    Entid,
    NamespacedKeyword,
    TypedValue,
    TypedValueConverter,
    ValueType,
};

use ::{
    DateTime,
    Queryable,
    QueryInputs,
    QueryOutput,
    Store,
    Uuid,
    Utc,
    Variable,
};

use errors::{
    Result,
};

pub struct QueryBuilder<'a> {
    sql: String,
    values: BTreeMap<Variable, TypedValue>,
    types: BTreeMap<Variable, ValueType>,
    store: &'a mut Store,
}

impl<'a> QueryBuilder<'a> {
    pub fn query<T>(store: &'a mut Store, sql: T) -> Result<QueryBuilder> where T: Into<String> {
        Ok(QueryBuilder { sql: sql.into(), values: BTreeMap::new(), types: BTreeMap::new(), store })
    }

    pub fn bind_value<T>(mut self, var: &str, value: T) -> Self where T: Into<TypedValue> {
        self.values.insert(Variable::from_valid_name(var), value.into());
        self
    }

    pub fn bind_ref_from_kw(mut self, var: &str, value: NamespacedKeyword) -> Result<Self> {
        let entid = self.store.conn().current_schema().get_entid(&value).ok_or(ErrorKind::UnknownAttribute(value.to_string()))?;
        self.values.insert(Variable::from_valid_name(var), TypedValue::Ref(entid.into()));
        Ok(self)
    }

    pub fn bind_ref<T>(mut self, var: &str, value: T) -> Self where T: Into<Entid> {
       self.values.insert(Variable::from_valid_name(var), TypedValue::Ref(value.into()));
       self
    }

    pub fn bind_type(mut self, var: &str, value_type: ValueType) -> Self {
        self.types.insert(Variable::from_valid_name(var), value_type);
        self
    }

    pub fn execute(&mut self) -> Result<QueryOutput> {
        let values = ::std::mem::replace(&mut self.values, Default::default());
        let types = ::std::mem::replace(&mut self.types, Default::default());
        let query_inputs = QueryInputs::new(types, values)?;
        let read = self.store.begin_read()?;
        read.q_once(&self.sql, query_inputs)
    }

    pub fn execute_scalar(mut self) -> Result<ScalarResult> {
        let results = self.execute()?;
        let scalar = ScalarResult(results.into_scalar()?);
        Ok(scalar)
    }

    pub fn execute_coll(mut self) -> Result<CollResult> {
        let results = self.execute()?;
        let coll = CollResult(results.into_coll()?);
        Ok(coll)
    }

    pub fn execute_tuple(mut self) -> Result<TupleResult> {
        let results = self.execute()?;
        let find_spec = results.spec.columns().map(|e| format!("{}", e)).collect();
        let tuple = TupleResult::new(&find_spec, results.into_tuple()?.unwrap_or(vec![]));
        Ok(tuple)
    }

    pub fn execute_rel(mut self) -> Result<RelResult> {
        let results = self.execute()?;
        let find_spec = results.spec.columns().map(|e| format!("{}", e)).collect();
        let tuple = RelResult::new(find_spec, results.into_rel()?);
        Ok(tuple)
    }
}

pub trait VarResultsRow {
    fn var_as_entid(&self, var: &str) -> Option<Entid>;
    fn var_as_kw(&self, var: &str) -> Option<Rc<NamespacedKeyword>>;
    fn var_as_boolean(&self, var: &str) -> Option<bool>;
    fn var_as_long(&self, var: &str) -> Option<i64>;
    fn var_as_double(&self, var: &str) -> Option<f64>;
    fn var_as_instant(&self, var: &str) -> Option<DateTime<Utc>>;
    fn var_as_timestamp(&self, var: &str) -> Option<i64>;
    fn var_as_string(&self, var: &str) -> Option<Rc<String>>;
    fn var_as_uuid(&self, var: &str) -> Option<Uuid>;
    fn var_as_uuid_str(&self, var: &str) -> Option<String>;
}

pub trait ColResultsRow {
    fn col_as_entid(&self, col: usize) -> Option<Entid>;
    fn col_as_kw(&self, col: usize) -> Option<Rc<NamespacedKeyword>>;
    fn col_as_boolean(&self, col: usize) -> Option<bool>;
    fn col_as_long(&self, col: usize) -> Option<i64>;
    fn col_as_double(&self, col: usize) -> Option<f64>;
    fn col_as_instant(&self, col: usize) -> Option<DateTime<Utc>>;
    fn col_as_timestamp(&self, col: usize) -> Option<i64>;
    fn col_as_string(&self, col: usize) -> Option<Rc<String>>;
    fn col_as_uuid(&self, col: usize) -> Option<Uuid>;
    fn col_as_uuid_string(&self, col: usize) -> Option<String>;
}

pub struct ScalarResult(Option<TypedValue>);

impl TypedValueConverter for ScalarResult {
    fn as_entid(mut self) -> Option<Entid> {
        self.0.take().map_or(None, |t| t.as_entid())
    }

    fn as_kw(mut self) -> Option<Rc<NamespacedKeyword>> {
        self.0.take().map_or(None, |t| t.as_kw())
    }

    fn as_boolean(mut self) -> Option<bool> {
        self.0.take().map_or(None, |t| t.as_boolean())
    }

    fn as_long(mut self) -> Option<i64> {
        self.0.take().map_or(None, |t| t.as_long())
    }

    fn as_double(mut self) -> Option<f64> {
        self.0.take().map_or(None, |t| t.as_double())
    }

    fn as_instant(mut self) -> Option<DateTime<Utc>> {
        self.0.take().map_or(None, |t| t.as_instant())
    }

    fn as_timestamp(mut self) -> Option<i64> {
        self.0.take().map_or(None, |t| t.as_timestamp())
    }

    fn as_string(mut self) -> Option<Rc<String>> {
        self.0.take().map_or(None, |t| t.as_string())
    }

    fn as_uuid(mut self) -> Option<Uuid> {
        self.0.take().map_or(None, |t| t.as_uuid())
    }

    fn as_uuid_string(mut self) -> Option<String> {
        self.0.take().map_or(None, |t| t.as_uuid_string())
    }
}

pub struct CollResult(pub Vec<TypedValue>);

impl CollResult {
    fn get_row(&self, row: usize) -> Option<TypedValue> {
        self.0.get(row).map(|v| v.to_owned())
    }

    pub fn iter(&self) -> ::std::slice::Iter<TypedValue> {
        self.0.iter()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl ColResultsRow for CollResult {
    fn col_as_entid(&self, row: usize) -> Option<Entid> {
        self.get_row(row).map_or(None, |t| t.as_entid())
    }

    fn col_as_kw(&self, row: usize) -> Option<Rc<NamespacedKeyword>> {
        self.get_row(row).map_or(None, |t| t.as_kw())
    }

    fn col_as_boolean(&self, row: usize) -> Option<bool> {
        self.get_row(row).map_or(None, |t| t.as_boolean())
    }

    fn col_as_long(&self, row: usize) -> Option<i64> {
        self.get_row(row).map_or(None, |t| t.as_long())
    }

    fn col_as_double(&self, row: usize) -> Option<f64> {
        self.get_row(row).map_or(None, |t| t.as_double())
    }

    fn col_as_instant(&self, row: usize) -> Option<DateTime<Utc>> {
        self.get_row(row).map_or(None, |t| t.as_instant())
    }

    fn col_as_timestamp(&self, row: usize) -> Option<i64> {
        self.get_row(row).map_or(None, |t| t.as_timestamp())
    }

    fn col_as_string(&self, row: usize) -> Option<Rc<String>> {
        self.get_row(row).map_or(None, |t| t.as_string())
    }

    fn col_as_uuid(&self, row: usize) -> Option<Uuid> {
        self.get_row(row).map_or(None, |t| t.as_uuid())
    }

    fn col_as_uuid_string(&self, row: usize) -> Option<String> {
        self.get_row(row).map_or(None, |t| t.as_uuid_string())
    }
}

impl IntoIterator for CollResult {
    type Item = TypedValue;
    type IntoIter = ::std::vec::IntoIter<TypedValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

pub struct TupleResult {
    map: IndexMap<String, TypedValue>,
}

impl TupleResult {
    pub fn new(spec: &Vec<String>, values: Vec<TypedValue>) -> TupleResult {
        let mut map = IndexMap::new();
        for (index, element) in spec.iter().enumerate() {
            if let Some(value) = values.get(index).take() {
                map.insert(element.clone(), value.to_owned());
            }
        }
        TupleResult {map}
    }

    pub fn iter(&self) -> indexmap::map::Iter<String, TypedValue> {
        self.map.iter()
    }

    fn get_value_for_var(&self, var: &str) -> Option<TypedValue> {
        self.map.get(var).map(|v| v.to_owned())
    }

    fn get_value_for_column(&self, col: usize) -> Option<TypedValue> {
        self.map.get_index(col).map(|(_k, v)| v.to_owned())
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }
}

impl VarResultsRow for TupleResult {
    fn var_as_entid(&self, var: &str) -> Option<Entid> {
        self.get_value_for_var(var).map_or(None, |v| v.as_entid())
    }

    fn var_as_kw(&self, var: &str) -> Option<Rc<NamespacedKeyword>> {
        self.get_value_for_var(var).map_or(None, |v| v.as_kw())
    }

    fn var_as_boolean(&self, var: &str) -> Option<bool> {
        self.get_value_for_var(var).map_or(None, |v| v.as_boolean())
    }

    fn var_as_long(&self, var: &str) -> Option<i64> {
        self.get_value_for_var(var).map_or(None, |v| v.as_long())
    }

    fn var_as_double(&self, var: &str) -> Option<f64> {
        self.get_value_for_var(var).map_or(None, |v| v.as_double())
    }

    fn var_as_instant(&self, var: &str) -> Option<DateTime<Utc>> {
        self.get_value_for_var(var).map_or(None, |v| v.as_instant())
    }

    fn var_as_timestamp(&self, var: &str) -> Option<i64> {
        self.get_value_for_var(var).map_or(None, |v| v.as_timestamp())
    }

    fn var_as_string(&self, var: &str) -> Option<Rc<String>> {
        self.get_value_for_var(var).map_or(None, |v| v.as_string())
    }

    fn var_as_uuid(&self, var: &str) -> Option<Uuid> {
        self.get_value_for_var(var).map_or(None, |v| v.as_uuid())
    }

    fn var_as_uuid_str(&self, var: &str) -> Option<String> {
        self.get_value_for_var(var).map_or(None, |v| v.as_uuid_string())
    }
}

impl ColResultsRow for TupleResult {
    fn col_as_entid(&self, col: usize) -> Option<Entid> {
        self.get_value_for_column(col).map_or(None, |v| v.as_entid())
    }

    fn col_as_kw(&self, col: usize) -> Option<Rc<NamespacedKeyword>> {
        self.get_value_for_column(col).map_or(None, |v| v.as_kw())
    }

    fn col_as_boolean(&self, col: usize) -> Option<bool> {
        self.get_value_for_column(col).map_or(None, |v| v.as_boolean())
    }

    fn col_as_long(&self, col: usize) -> Option<i64> {
        self.get_value_for_column(col).map_or(None, |v| v.as_long())
    }

    fn col_as_double(&self, col: usize) -> Option<f64> {
        self.get_value_for_column(col).map_or(None, |v| v.as_double())
    }

    fn col_as_instant(&self, col: usize) -> Option<DateTime<Utc>> {
        self.get_value_for_column(col).map_or(None, |v| v.as_instant())
    }

    fn col_as_timestamp(&self, col: usize) -> Option<i64> {
        self.get_value_for_column(col).map_or(None, |v| v.as_timestamp())
    }

    fn col_as_string(&self, col: usize) -> Option<Rc<String>> {
        self.get_value_for_column(col).map_or(None, |v| v.as_string())
    }

    fn col_as_uuid(&self, col: usize) -> Option<Uuid> {
        self.get_value_for_column(col).map_or(None, |v| v.as_uuid())
    }

    fn col_as_uuid_string(&self, col: usize) -> Option<String> {
        self.get_value_for_column(col).map_or(None, |v| v.as_uuid_string())
    }
}

impl IntoIterator for TupleResult {
    type Item = (String, TypedValue);
    type IntoIter = indexmap::map::IntoIter<String, TypedValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.map.into_iter()
    }
}

pub struct RelResult {
    rows: Vec<TupleResult>,
}

impl RelResult {
    pub fn new(spec: Vec<String>, values: Vec<Vec<TypedValue>>) -> RelResult {
        let res = values.into_iter().map(|v| TupleResult::new(&spec, v)).collect();
        RelResult { rows: res }
    }

    pub fn iter(&self) -> ::std::slice::Iter<TupleResult> {
        self.rows.iter()
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }
}

impl IntoIterator for RelResult {
    type Item = TupleResult;
    type IntoIter = ::std::vec::IntoIter<TupleResult>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.into_iter()
    }
}

mod test {
    use super::{
        ColResultsRow,
        QueryBuilder,
        TypedValue,
        TypedValueConverter,
        Store,
        VarResultsRow,
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

        let entid = QueryBuilder::query(&mut store, r#"[:find ?x .
                                              :in ?v
                                              :where [?x :foo/boolean ?v]]"#).expect("QueryBuilder")
                              .bind_value("?v", true)
                              .execute_scalar().expect("ScalarResult")
                              .as_entid();

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

        let entids: Vec<i64> = QueryBuilder::query(&mut store, r#"[:find [?x ...]
                                              :in ?v
                                              :where [?x :foo/boolean ?v]]"#).expect("QueryBuilder")
                              .bind_value("?v", true)
                              .execute_coll().expect("CollResult")
                              .into_iter()
                              .map(|v| v.as_entid().expect("val"))
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

        let entid = QueryBuilder::query(&mut store, r#"[:find [?x ...]
                                              :in ?v
                                              :where [?x :foo/boolean ?v]]"#).expect("QueryBuilder")
                              .bind_value("?v", true)
                              .execute_coll().expect("CollResult")
                              .col_as_entid(1).expect("entid");

        assert_eq!(entid, n_yes);
    }

    #[test]
    fn test_tuple_query_result_by_var() {
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

        let results = QueryBuilder::query(&mut store, r#"[:find [?x, ?i]
                                              :in ?v ?i
                                              :where [?x :foo/boolean ?v]
                                                     [?x :foo/long ?i]]"#).expect("QueryBuilder")
                              .bind_value("?v", true)
                              .bind_value("?i", 27)
                              .execute_tuple().expect("TupleResult");
        let entid = results.var_as_entid("?x").expect("entid");
        let long_val = results.var_as_long("?i").expect("long");

        assert_eq!(entid, n_yes);
        assert_eq!(long_val, 27);
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

        let results = QueryBuilder::query(&mut store, r#"[:find [?x, ?i]
                                              :in ?v ?i
                                              :where [?x :foo/boolean ?v]
                                                     [?x :foo/long ?i]]"#).expect("QueryBuilder")
                              .bind_value("?v", true)
                              .bind_value("?i", 27)
                              .execute_tuple().expect("TupleResult");
        let entid = results.col_as_entid(0).expect("entid");
        let long_val = results.col_as_long(1).expect("long");

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

        let results: Vec<TypedValue> = QueryBuilder::query(&mut store, r#"[:find [?x, ?i]
                                              :in ?v ?i
                                              :where [?x :foo/boolean ?v]
                                                     [?x :foo/long ?i]]"#).expect("QueryBuilder")
                              .bind_value("?v", true)
                              .bind_value("?i", 27)
                              .execute_tuple().expect("TupleResult")
                              .into_iter()
                              .map(|(_var, t)| t)
                              .collect();
        let entid = TypedValue::Ref(n_yes.clone());
        let long_val = TypedValue::Long(27);

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

        let mut results: Vec<Res> = QueryBuilder::query(&mut store, r#"[:find ?x ?v ?i
                                              :where [?x :foo/boolean ?v]
                                                     [?x :foo/long ?i]]"#).expect("QueryBuilder")
                              .execute_rel().expect("RelResult")
                              .into_iter()
                              .map(|row| {
                                  Res {
                                      entid: row.col_as_entid(0).expect("entid"),
                                      boolean: row.var_as_boolean("?v").expect("boolean"),
                                      long_val: row.var_as_long("?i").expect("long"),
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

        #[derive(Debug, PartialEq)]
        struct Res {
            entid: i64,
            boolean: bool,
            long_val: i64,
        };

        let results = QueryBuilder::query(&mut store, r#"[:find [?v ?i]
                                              :in ?x
                                              :where [?x :foo/boolean ?v]
                                                     [?x :foo/long ?i]]"#).expect("QueryBuilder")
                              .bind_ref("?x", l_yes)
                              .execute_tuple().expect("TupleResult");
        assert_eq!(results.var_as_boolean("?v").expect("boolean"), true);
        assert_eq!(results.var_as_long("?i").expect("long"), 25);
    }
}
