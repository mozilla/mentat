// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

///! This module defines some core types that support find expressions: sources,
///! variables, expressions, etc.
///! These are produced as 'fuel' by the query parser, consumed by the query
///! translator and executor.
///!
///! Many of these types are defined as simple structs that are little more than
///! a richer type alias: a variable, for example, is really just a fancy kind
///! of string.
///!
///! At some point in the future, we might consider reducing copying and memory
///! usage by recasting all of these string-holding structs and enums in terms
///! of string references, with those references being slices of some parsed
///! input query string, and valid for the lifetime of that string.
///!
///! For now, for the sake of simplicity, all of these strings are heap-allocated.
///!
///! Furthermore, we might cut out some of the chaff here: each time a 'tagged'
///! type is used within an enum, we have an opportunity to simplify and use the
///! inner type directly in conjunction with matching on the enum. Before diving
///! deeply into this it's worth recognizing that this loss of 'sovereignty' is
///! a tradeoff against well-typed function signatures and other such boundaries.

extern crate edn;
extern crate num;
extern crate ordered_float;

use num::BigInt;
use ordered_float::OrderedFloat;
use edn::{Keyword, NamespacedKeyword, PlainSymbol};

pub type SrcVarName = String;          // Do not include the required syntactic '$'.

pub struct Variable(pub PlainSymbol);

#[derive(Clone,Debug,Eq,PartialEq)]
pub enum SrcVar {
    DefaultSrc,
    NamedSrc(SrcVarName),
}

/// These are the scalar values representable in EDN.
#[derive(Clone,Debug,Eq,PartialEq)]
pub enum NonIntegerConstant {
    Boolean(bool),
    BigInteger(BigInt),
    Float(OrderedFloat<f64>),
    Text(String),
}

pub enum FnArg {
    Variable(Variable),
    SrcVar(SrcVar),
    EntidOrInteger(i64),
    Ident(NamespacedKeyword),
    Constant(NonIntegerConstant),
}

/// e, a, tx can't be values -- no strings, no floats -- and so
/// they can only be variables, entity IDs, ident keywords, or
/// placeholders.
/// This encoding allows us to represent integers that aren't
/// entity IDs. That'll get filtered out in the context of the
/// database.
pub enum PatternNonValuePlace {
    Placeholder,
    Variable(Variable),
    Entid(u64),                       // Note unsigned.
    Ident(NamespacedKeyword),
}

/// The `v` part of a pattern can be much broader: it can represent
/// integers that aren't entity IDs (particularly negative integers),
/// strings, and all the rest. We group those under `Constant`.
pub enum PatternValuePlace {
    Placeholder,
    Variable(Variable),
    EntidOrInteger(i64),
    Ident(NamespacedKeyword),
    Constant(NonIntegerConstant),
}

/*
pub enum PullPattern {
    Constant(Constant),
    Variable(Variable),
}

pub struct Pull {
    pub src: SrcVar,
    pub var: Variable,
    pub pattern: PullPattern,      // Constant, variable, or plain variable.
}
*/

/*
pub struct Aggregate {
    pub fn_name: String,
    pub args: Vec<FnArg>,
}
*/

pub enum Element {
    Variable(Variable),
    // Aggregate(Aggregate),   // TODO
    // Pull(Pull),             // TODO
}

/// A definition of the first part of a find query: the
/// `[:find ?foo ?bar…]` bit.
///
/// There are four different kinds of find specs, allowing you to query for
/// a single value, a collection of values from different entities, a single
/// tuple (relation), or a collection of tuples.
///
/// Examples:
///
/// ```rust
/// # extern crate edn;
/// # extern crate mentat_query;
/// # use edn::PlainSymbol;
/// # use mentat_query::{Element, FindSpec, Variable};
///
/// # fn main() {
///
///   let elements = vec![
///     Element::Variable(Variable(PlainSymbol("?foo".to_string()))),
///     Element::Variable(Variable(PlainSymbol("?bar".to_string()))),
///   ];
///   let rel = FindSpec::FindRel(elements);
///
///   if let FindSpec::FindRel(elements) = rel {
///     assert_eq!(2, elements.len());
///   }
///
/// # }
/// ```
///
pub enum FindSpec {
    /// Returns an array of arrays.
    FindRel(Vec<Element>),

    /// Returns an array of scalars, usually homogeneous.
    /// This is equivalent to mapping over the results of a `FindRel`,
    /// returning the first value of each.
    FindColl(Element),

    /// Returns a single tuple: a heterogeneous array of scalars. Equivalent to
    /// taking the first result from a `FindRel`.
    FindTuple(Vec<Element>),

    /// Returns a single scalar value. Equivalent to taking the first result
    /// from a `FindColl`.
    FindScalar(Element),
}

/// Returns true if the provided `FindSpec` returns at most one result.
pub fn is_unit_limited(spec: &FindSpec) -> bool {
    match spec {
        &FindSpec::FindScalar(..) => true,
        &FindSpec::FindTuple(..)  => true,
        &FindSpec::FindRel(..)    => false,
        &FindSpec::FindColl(..)   => false,
    }
}

/// Returns true if the provided `FindSpec` cares about distinct results.
pub fn requires_distinct(spec: &FindSpec) -> bool {
    return !is_unit_limited(spec);
}

// Note that the "implicit blank" rule applies.
// A pattern with a reversed attribute — :foo/_bar — is reversed
// at the point of parsing. These `Pattern` instances only represent
// one direction.
pub struct Pattern {
    source: Option<SrcVar>,
    entity: PatternNonValuePlace,
    attribute: PatternNonValuePlace,
    value: PatternValuePlace,
    tx: PatternNonValuePlace,
}

pub enum WhereClause {
    /*
    Not,
    NotJoin,
    Or,
    OrJoin,
    Pred,
    WhereFn,
    RuleExpr,
    */
    Pattern,
}

pub struct Query {
    find: FindSpec,
    with: Vec<Variable>,
    in_vars: Vec<Variable>,
    in_sources: Vec<SrcVar>,
    where_clauses: Vec<WhereClause>,
    // TODO: in_rules;
}
