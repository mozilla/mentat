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
use mentat_core::{
    Entid,
};

use mentat_query::{
    Variable,
};

use errors::{
    Result,
};

pub enum EntityType {
    Predicate(String),
    Ref(Entid),
    Var(String),
}

pub enum AttributeType {
    Keyword(String),
    Ref(Entid),
    Var(String),
}

pub enum QueryValueType {
    Boolean(bool),
    Double(f64),
    Instant(i64),
    Keyword(String),
    Long(i64),
    Ref(Entid),
    String(String),
    Uuid(String),
    Var(String),
}

pub enum FindType {
    Coll,
    Rel,
    Scalar,
    Tuple,
}

#[derive(Default)]
pub struct QueryBuilder {
    find_builder: Option<FindBuilder>,
    where_builder: Option<WhereBuilder>,
    order_builder: Option<OrderBuilder>,
    limit: Option<i32>,
}

pub struct FindBuilder {
    find_type: FindType,
    vars: Vec<Variable>,
}

#[derive(Default)]
pub struct WhereBuilder {
    clauses: Vec<Box<ClauseBuilder>>,
}

pub trait ClauseBuilder {}

#[derive(Default)]
pub struct WhereClauseBuilder {
    entity: Option<EntityType>,
    attribute: Option<AttributeType>,
    value: Option<QueryValueType>,
}

impl ClauseBuilder for WhereClauseBuilder {}

#[derive(Default)]
pub struct OrClauseBuilder {
    clauses: Vec<Box<ClauseBuilder>>,
    join: Option<Variable>,
}

impl ClauseBuilder for OrClauseBuilder {}

#[derive(Default)]
pub struct NotClauseBuilder {
    clauses: Vec<Box<ClauseBuilder>>,
    join: Option<Variable>,
}

impl ClauseBuilder for NotClauseBuilder {}

#[derive(Default)]
pub struct AndClauseBuilder {
    clauses: Vec<Box<ClauseBuilder>>,
}

impl ClauseBuilder for AndClauseBuilder {}

enum QueryOrder {
    Ascending,
    Descending,
}

#[derive(Default)]
pub struct OrderBuilder {
    orders: Vec<(Variable, QueryOrder)>,
}

impl QueryBuilder {
    pub fn add_find<F>(mut self, builder_fn: F) -> Self where F: 'static + FnOnce(FindBuilder) -> FindBuilder {
        self.find_builder = Some(builder_fn(FindBuilder::new()));
        // self.find_builder = Some(builder);
        self
    }

    pub fn add_where<F>(mut self, builder_fn: F) -> Self where F: 'static + FnOnce(WhereBuilder) -> WhereBuilder {
        self.where_builder = Some(builder_fn(WhereBuilder::default()));
        self
    }

    pub fn add_order<F>(mut self, builder_fn: F) -> Self where F: 'static + FnOnce(OrderBuilder) -> OrderBuilder {
        self.order_builder = Some(builder_fn(OrderBuilder::default()));
        self
    }

    pub fn add_limit(mut self, limit: i32) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn execute(self) -> Result<()> {
        println!("executing and destroying self in process");
        Ok(())
    }
}

impl FindBuilder {
    pub fn with_type(find_type: FindType) -> Self {
        FindBuilder {
            find_type: find_type,
            vars: vec![],
        }
    }

    pub fn new() -> Self {
        FindBuilder {
            find_type: FindType::Rel,
            vars: vec![],
        }
    }

    pub fn set_type(mut self, find_type: FindType) -> Self {
        self.find_type = find_type;
        self
    }

    pub fn add<'a, T>(mut self, var: T) -> Self where T: Into<&'a str> {
        self.vars.push(Variable::from_valid_name(var.into()));
        self
    }
}

impl WhereBuilder {
    pub fn add<T>(mut self, clause: T) -> Self where T: 'static + ClauseBuilder {
        self.clauses.push(Box::new(clause));
        self
    }

    pub fn add_clause<E, A, V>(mut self, entity: E, attribute: A, value: V) -> Self
    where E: Into<Option<EntityType>>,
          A: Into<Option<AttributeType>>,
          V: Into<Option<QueryValueType>> {
        self.clauses.push(Box::new( WhereClauseBuilder{
            entity: entity.into(),
            attribute: attribute.into(),
            value: value.into(),
        }));
        self
    }

    pub fn add_or<F>(mut self, or_fn: F) -> Self where F: 'static + FnOnce(OrClauseBuilder) -> OrClauseBuilder {
        self.clauses.push(Box::new(or_fn(OrClauseBuilder::default())));
        self
    }

    pub fn add_not<F>(mut self, not_fn: F) -> Self where F: 'static + FnOnce(NotClauseBuilder) -> NotClauseBuilder {
        self.clauses.push(Box::new(not_fn(NotClauseBuilder::default())));
        self
    }
}

impl WhereClauseBuilder {
    pub fn add_entity(mut self, entity: EntityType) -> Self {
        self.entity = Some(entity);
        self
    }

    pub fn add_attribute(mut self, attribute: AttributeType) -> Self {
        self.attribute = Some(attribute);
        self
    }

    pub fn add_value(mut self, value: QueryValueType) -> Self {
        self.value = Some(value);
        self
    }
}

impl NotClauseBuilder {
    pub fn add<T>(mut self, clause: T) -> Self where T: 'static + ClauseBuilder {
        self.clauses.push(Box::new(clause));
        self
    }

    pub fn join(mut self, var: &str) -> Self {
        self.join = Some(Variable::from_valid_name(var));
        self
    }

    pub fn add_clause<E, A, V>(mut self, entity: E, attribute: A, value: V) -> Self
    where E: Into<Option<EntityType>>,
          A: Into<Option<AttributeType>>,
          V: Into<Option<QueryValueType>> {
        self.clauses.push(Box::new( WhereClauseBuilder{
            entity: entity.into(),
            attribute: attribute.into(),
            value: value.into(),
        }));
        self
    }

    pub fn add_or<F>(mut self, or_fn: F) -> Self where F: 'static + FnOnce(OrClauseBuilder) -> OrClauseBuilder {
        self.clauses.push(Box::new(or_fn(OrClauseBuilder::default())));
        self
    }

    pub fn add_not<F>(mut self, not_fn: F) -> Self where F: 'static + FnOnce(NotClauseBuilder) -> NotClauseBuilder {
        self.clauses.push(Box::new(not_fn(NotClauseBuilder::default())));
        self
    }

    pub fn add_and<F>(mut self, and_fn: F) -> Self where F: 'static + FnOnce(AndClauseBuilder) -> AndClauseBuilder {
        self.clauses.push(Box::new(and_fn(AndClauseBuilder::default())));
        self
    }
}

impl OrClauseBuilder {
    pub fn add<T>(mut self, clause: T) -> Self where T: 'static + ClauseBuilder {
        self.clauses.push(Box::new(clause));
        self
    }

    pub fn join(mut self, var: &str) -> Self {
        self.join = Some(Variable::from_valid_name(var));
        self
    }

    pub fn add_clause<E, A, V>(mut self, entity: E, attribute: A, value: V) -> Self
    where E: Into<Option<EntityType>>,
          A: Into<Option<AttributeType>>,
          V: Into<Option<QueryValueType>> {
        self.clauses.push(Box::new( WhereClauseBuilder{
            entity: entity.into(),
            attribute: attribute.into(),
            value: value.into(),
        }));
        self
    }

    pub fn add_or<F>(mut self, or_fn: F) -> Self where F: 'static + FnOnce(OrClauseBuilder) -> OrClauseBuilder {
        self.clauses.push(Box::new(or_fn(OrClauseBuilder::default())));
        self
    }

    pub fn add_not<F>(mut self, not_fn: F) -> Self where F: 'static + FnOnce(NotClauseBuilder) -> NotClauseBuilder {
        self.clauses.push(Box::new(not_fn(NotClauseBuilder::default())));
        self
    }

    pub fn add_and<F>(mut self, and_fn: F) -> Self where F: 'static + FnOnce(AndClauseBuilder) -> AndClauseBuilder {
        self.clauses.push(Box::new(and_fn(AndClauseBuilder::default())));
        self
    }
}

impl AndClauseBuilder {
    pub fn add<T>(mut self, clause: T) -> Self where T: 'static + ClauseBuilder {
        self.clauses.push(Box::new(clause));
        self
    }

    pub fn add_clause<E, A, V>(mut self, entity: E, attribute: A, value: V) -> Self
    where E: Into<Option<EntityType>>,
          A: Into<Option<AttributeType>>,
          V: Into<Option<QueryValueType>> {
        self.clauses.push(Box::new( WhereClauseBuilder{
            entity: entity.into(),
            attribute: attribute.into(),
            value: value.into(),
        }));
        self
    }

    pub fn add_or<F>(mut self, or_fn: F) -> Self where F: 'static + FnOnce(OrClauseBuilder) -> OrClauseBuilder {
        self.clauses.push(Box::new(or_fn(OrClauseBuilder::default())));
        self
    }

    pub fn add_not<F>(mut self, not_fn: F) -> Self where F: 'static + FnOnce(NotClauseBuilder) -> NotClauseBuilder {
        self.clauses.push(Box::new(not_fn(NotClauseBuilder::default())));
        self
    }

    pub fn add_and<F>(mut self, and_fn: F) -> Self where F: 'static + FnOnce(AndClauseBuilder) -> AndClauseBuilder {
        self.clauses.push(Box::new(and_fn(AndClauseBuilder::default())));
        self
    }
}

impl OrderBuilder {
    pub fn add<'a, T>(self, var: T) -> Self where T: Into<&'a str> {
        self.add_ascending(var)
    }

    pub fn add_ascending<'a, T>(mut self, var: T) -> Self where T: Into<&'a str> {
        self.orders.push((Variable::from_valid_name(var.into()), QueryOrder::Ascending));
        self
    }

    pub fn add_descending<'a, T>(mut self, var: T) -> Self where T: Into<&'a str> {
        self.orders.push((Variable::from_valid_name(var.into()), QueryOrder::Descending));
        self
    }
}

pub fn query() -> QueryBuilder {
    QueryBuilder::default()
}


#[cfg(test)]
mod test {
    extern crate mentat_db;

    use query_builder::*;

    #[test]
    // [:find ?x :where [?x :foo/bar "yyy"]]
    fn test_find_rel() {
        let _ = query().add_find(|find| find.add("?x"))
                              .add_where(|w| {
                                    w.add_clause(EntityType::Var("?x".to_string()), AttributeType::Keyword(":foo/bar".to_string()), QueryValueType::String("yyy".to_string()))
                              }).execute();
        panic!("not complete");
    }

    #[test]
    // [:find ?x :where [?x _ "yyy"]]
    fn test_find_no_attribute() {
        let _ = query().add_find(|find| find.add("?x"))
                              .add_where(|w| {
                                    w.add_clause(EntityType::Var("?x".to_string()), None, QueryValueType::String("yyy".to_string()))
                              }).execute();
        panic!("not complete");
    }

    #[test]
    // [:find ?x . :where [?x :foo/bar "yyy"]]
    fn test_find_scalar() {
        let _ = query().add_find(|find| find.set_type(FindType::Scalar).add("?x"))
                              .add_where(|w| {
                                    w.add_clause(EntityType::Var("?x".to_string()), AttributeType::Keyword(":foo/bar".to_string()), QueryValueType::String("yyy".to_string()))
                              }).execute();
        panic!("not complete");
    }


    // [:find [?url ?description]
    //  :where
    //  (or-join [?page]
    //     [?page :page/url "http://foo.com/"]
    //     [?page :page/title "Foo"])
    // [?page :page/url ?url]
    // [?page :page/description ?description]]
    #[test]
    fn test_find_or_join() {
        let _ = query().add_find(|find| find.add("?url").add("?description"))
                       .add_where(|w| {
                           w.add_or(|or| {
                               or.join("?page")
                                   .add_clause(EntityType::Var("?page".to_string()), AttributeType::Keyword(":page/url".to_string()), QueryValueType::String("http://foo.com/".to_string()))
                                   .add_clause(EntityType::Var("?page".to_string()), AttributeType::Keyword(":page/title".to_string()), QueryValueType::String("Foo".to_string()))
                            })
                            .add_clause(EntityType::Var("?page".to_string()), AttributeType::Keyword(":page/url".to_string()), QueryValueType::Var("?url".to_string()))
                            .add_clause(EntityType::Var("?page".to_string()), AttributeType::Keyword(":page/description".to_string()), QueryValueType::Var("?description".to_string()))
                       })
                       .execute();
        panic!("not complete");
    }


    // [:find ?x :where [?x :foo/baz ?y] :limit 1000]
    #[test]
    fn test_find_with_limit() {
        let _ = query().add_find(|find| find.add("?x"))
                       .add_where(|w| {
                           w.add_clause(EntityType::Var("?x".to_string()), AttributeType::Keyword(":foo/baz".to_string()), QueryValueType::Var("?y".to_string()))
                       })
                       .add_limit(1000)
                       .execute();
        panic!("not complete");
    }

    // [:find ?x :where [?x :foo/baz ?y] :order ?y]
    #[test]
    fn test_find_with_default_order() {
        let _ = query().add_find(|find| find.add("?x"))
                       .add_where(|w| {
                           w.add_clause(EntityType::Var("?x".to_string()), AttributeType::Keyword(":foo/baz".to_string()), QueryValueType::Var("?y".to_string()))
                       })
                       .add_order(|order| order.add("?y"))
                       .execute();
        panic!("not complete");
    }

    // [:find ?x :where [?x :foo/bar ?y] :order (desc ?y)]
    #[test]
    fn test_find_with_desc_order() {
        let _ = query().add_find(|find| find.add("?x"))
                       .add_where(|w| {
                           w.add_clause(EntityType::Var("?x".to_string()), AttributeType::Keyword(":foo/bar".to_string()), QueryValueType::Var("?y".to_string()))
                       })
                       .add_order(|order| order.add_descending("?y"))
                       .execute();
        panic!("not complete");
    }

    // [:find ?x :where [?x :foo/baz ?y] :order (desc ?y) (asc ?x)]
    #[test]
    fn test_find_with_multiple_orders() {
        let _ = query().add_find(|find| find.add("?x"))
                       .add_where(|w| {
                           w.add_clause(EntityType::Var("?x".to_string()), AttributeType::Keyword(":foo/baz".to_string()), QueryValueType::Var("?y".to_string()))
                       })
                       .add_order(|order| order.add_descending("?y").add_ascending("?x"))
                       .execute();
        panic!("not complete");
    }

    // [:find ?x . :where [?x :foo/bar ?y] [(!= ?y 12)]]
    #[test]
    fn test_find_with_predicate() {
        let _ = query().add_find(|find| find.set_type(FindType::Scalar).add("?x"))
                       .add_where(|w| {
                           w.add_clause(EntityType::Var("?x".to_string()), AttributeType::Keyword(":foo/bar".to_string()), QueryValueType::Var("?y".to_string()))
                               .add_clause(EntityType::Predicate("!=".to_string()), AttributeType::Var("?y".to_string()), QueryValueType::Long(12))
                       })
                       .execute();
        panic!("not complete");
    }

    // figure out ground!
    #[test]
    fn test_find_with_ground() {
        panic!("not complete");
    }

    // figure out fulltext!
    #[test]
    fn test_find_with_fulltext() {
        panic!("not complete");
    }
}
