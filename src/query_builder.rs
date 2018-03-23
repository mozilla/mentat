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
    pub fn add_find(mut self, builder: FindBuilder) -> Self {
        self.find_builder = Some(builder);
        self
    }

    pub fn add_where(mut self, builder: WhereBuilder) -> Self {
        self.where_builder = Some(builder);
        self
    }

    pub fn add_order(mut self, builder: OrderBuilder) -> Self {
        self.order_builder = Some(builder);
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

    pub fn add_type(mut self, find_type: FindType) -> Self {
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
}

impl AndClauseBuilder {
    pub fn add<T>(mut self, clause: T) -> Self where T: 'static + ClauseBuilder {
        self.clauses.push(Box::new(clause));
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

    // [:find ?x :where [?x :foo/bar "yyy"]]
    fn test_find_rel() {
        let find = FindBuilder::new().add("?x");
        let clause = WhereClauseBuilder::default().add_entity(EntityType::Var("?x".to_string()))
                                                  .add_attribute(AttributeType::Keyword(":foo/bar".to_string()))
                                                  .add_value(QueryValueType::String("yyy".to_string()));
        let where_clause = WhereBuilder::default().add(clause);
        let response = query().add_find(find).add_where(where_clause).execute();
        panic!("not complete");
    }

    // [:find ?x . :where [?x :foo/bar "yyy"]]
    fn test_find_scalar() {
        let find = FindBuilder::with_type(FindType::Scalar).add("?x");
        let clause = WhereClauseBuilder::default().add_entity(EntityType::Var("?x".to_string()))
                                                  .add_attribute(AttributeType::Keyword(":foo/bar".to_string()))
                                                  .add_value(QueryValueType::String("yyy".to_string()));
        let where_clause = WhereBuilder::default().add(clause);
        let response = query().add_find(find).add_where(where_clause).execute();
        panic!("not complete");
    }


    // [:find [?url ?description]
    //  :where
    //  (or-join [?page]
    //     [?page :page/url "http://foo.com/"]
    //     [?page :page/title "Foo"])
    // [?page :page/url ?url]
    // [?page :page/description ?description]]
    fn test_find_or_join() {
        let find = FindBuilder::with_type(FindType::Scalar).add("?url")
                                                           .add("?description");

        let or_url_clause = WhereClauseBuilder::default().add_entity(EntityType::Var("?page".to_string()))
                                                         .add_attribute(AttributeType::Keyword(":page/url".to_string()))
                                                         .add_value(QueryValueType::String("http://foo.com/".to_string()));
        let or_title_clause = WhereClauseBuilder::default().add_entity(EntityType::Var("?page".to_string()))
                                                           .add_attribute(AttributeType::Keyword(":page/title".to_string()))
                                                           .add_value(QueryValueType::String("Foo".to_string()));
        let or_clause = OrClauseBuilder::default().join("?page")
                                                  .add(or_url_clause)
                                                  .add(or_title_clause);

        let url_clause = WhereClauseBuilder::default().add_entity(EntityType::Var("?page".to_string()))
                                                      .add_attribute(AttributeType::Keyword(":page/url".to_string()))
                                                      .add_value(QueryValueType::Var("?url".to_string()));
        let description_clause = WhereClauseBuilder::default().add_entity(EntityType::Var("?page".to_string()))
                                                              .add_attribute(AttributeType::Keyword(":page/description".to_string()))
                                                              .add_value(QueryValueType::Var("?description".to_string()));

        let where_clause = WhereBuilder::default().add(or_clause)
                                                  .add(url_clause)
                                                  .add(description_clause);

        let response = query().add_find(find)
                              .add_where(where_clause).execute();
        panic!("not complete");
    }


    /// [:find ?x :where [?x :foo/baz ?y] :limit 1000]
    fn test_find_with_limit() {
        let find = FindBuilder::new().add("?x");
        let clause = WhereClauseBuilder::default().add_entity(EntityType::Var("?x".to_string()))
                                                  .add_attribute(AttributeType::Keyword(":foo/baz".to_string()))
                                                  .add_value(QueryValueType::Var("?y".to_string()));
        let where_clause = WhereBuilder::default().add(clause);
        let response = query().add_find(find).add_where(where_clause).add_limit(1000).execute();
    }

    /// [:find ?x :where [?x :foo/baz ?y] :order ?y]
    fn test_find_with_default_order() {
        let find = FindBuilder::new().add("?x");
        let clause = WhereClauseBuilder::default().add_entity(EntityType::Var("?x".to_string()))
                                                  .add_attribute(AttributeType::Keyword(":foo/baz".to_string()))
                                                  .add_value(QueryValueType::Var("?y".to_string()));
        let where_clause = WhereBuilder::default().add(clause);
        let order_clause = OrderBuilder::default().add("?y");
        let response = query().add_find(find).add_where(where_clause).add_order(order_clause).execute();
    }

    /// [:find ?x :where [?x :foo/bar ?y] :order (desc ?y)]
    fn test_find_with_desc_order() {
        let find = FindBuilder::new().add("?x");
        let clause = WhereClauseBuilder::default().add_entity(EntityType::Var("?x".to_string()))
                                                  .add_attribute(AttributeType::Keyword(":foo/bar".to_string()))
                                                  .add_value(QueryValueType::Var("?y".to_string()));
        let where_clause = WhereBuilder::default().add(clause);
        let order_clause = OrderBuilder::default().add_descending("?y");
        let response = query().add_find(find).add_where(where_clause).add_order(order_clause).execute();
    }

    /// [:find ?x :where [?x :foo/baz ?y] :order (desc ?y) (asc ?x)]
    fn test_find_with_multiple_orders() {
        let find = FindBuilder::new().add("?x");
        let clause = WhereClauseBuilder::default().add_entity(EntityType::Var("?x".to_string()))
                                                  .add_attribute(AttributeType::Keyword(":foo/baz".to_string()))
                                                  .add_value(QueryValueType::Var("?y".to_string()));
        let where_clause = WhereBuilder::default().add(clause);
        let order_clause = OrderBuilder::default().add_descending("?y")
                                                  .add_ascending("?x");
        let response = query().add_find(find).add_where(where_clause).add_order(order_clause).execute();
    }

    /// [:find ?x . :where [?x :foo/bar ?y] [(!= ?y 12)]]
    fn test_find_with_predicate() {
        let find = FindBuilder::with_type(FindType::Scalar).add("?x");
        let eav_clause = WhereClauseBuilder::default().add_entity(EntityType::Var("?x".to_string()))
                                                  .add_attribute(AttributeType::Keyword(":foo/bar".to_string()))
                                                  .add_value(QueryValueType::Var("?y".to_string()));
        let predicate_clause = WhereClauseBuilder::default().add_entity(EntityType::Predicate("!=".to_string()))
                                                  .add_attribute(AttributeType::Var("?y".to_string()))
                                                  .add_value(QueryValueType::Long(12));
        let where_clause = WhereBuilder::default().add(eav_clause)
                                                  .add(predicate_clause);
        let response = query().add_find(find).add_where(where_clause).execute();
    }

    /// figure out ground!
    fn test_find_with_ground() {

    }
}
