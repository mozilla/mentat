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
extern crate mentat_core;

use std::fmt;
use edn::{BigInt, OrderedFloat};
pub use edn::{NamespacedKeyword, PlainSymbol};
use mentat_core::TypedValue;

pub type SrcVarName = String;          // Do not include the required syntactic '$'.

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Variable(pub PlainSymbol);

pub trait FromValue<T> {
    fn from_value(v: &edn::Value) -> Option<T>;
}

/// If the provided EDN value is a PlainSymbol beginning with '?', return
/// it wrapped in a Variable. If not, return None.
impl FromValue<Variable> for Variable {
    fn from_value(v: &edn::Value) -> Option<Variable> {
        if let edn::Value::PlainSymbol(ref s) = *v {
            Variable::from_symbol(s)
        } else {
            None
        }
    }
}

impl Variable {
    pub fn from_symbol(sym: &PlainSymbol) -> Option<Variable> {
        if sym.is_var_symbol() {
            Some(Variable(sym.clone()))
        } else {
            None
        }
    }
}

impl fmt::Debug for Variable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "var({})", self.0)
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PredicateFn(pub PlainSymbol);

impl FromValue<PredicateFn> for PredicateFn {
    fn from_value(v: &edn::Value) -> Option<PredicateFn> {
        if let edn::Value::PlainSymbol(ref s) = *v {
            PredicateFn::from_symbol(s)
        } else {
            None
        }
    }
}

impl PredicateFn {
    pub fn from_symbol(sym: &PlainSymbol) -> Option<PredicateFn> {
        // TODO: validate the acceptable set of function names.
        Some(PredicateFn(sym.clone()))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SrcVar {
    DefaultSrc,
    NamedSrc(SrcVarName),
}

impl FromValue<SrcVar> for SrcVar {
    fn from_value(v: &edn::Value) -> Option<SrcVar> {
        if let edn::Value::PlainSymbol(ref s) = *v {
            SrcVar::from_symbol(s)
        } else {
            None
        }
    }
}

impl SrcVar {
    pub fn from_symbol(sym: &PlainSymbol) -> Option<SrcVar> {
        if sym.is_src_symbol() {
            Some(SrcVar::NamedSrc(sym.plain_name().to_string()))
        } else {
            None
        }
    }
}

/// These are the scalar values representable in EDN.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NonIntegerConstant {
    Boolean(bool),
    BigInteger(BigInt),
    Float(OrderedFloat<f64>),
    Text(String),
}

impl NonIntegerConstant {
    pub fn into_typed_value(self) -> TypedValue {
        match self {
            NonIntegerConstant::BigInteger(_) => unimplemented!(),     // TODO: #280.
            NonIntegerConstant::Boolean(v) => TypedValue::Boolean(v),
            NonIntegerConstant::Float(v) => TypedValue::Double(v),
            NonIntegerConstant::Text(v) => TypedValue::String(v),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FnArg {
    Variable(Variable),
    SrcVar(SrcVar),
    EntidOrInteger(i64),
    Ident(NamespacedKeyword),
    Constant(NonIntegerConstant),
}

impl FromValue<FnArg> for FnArg {
    fn from_value(v: &edn::Value) -> Option<FnArg> {
        // TODO: support SrcVars.
        Variable::from_value(v)
                 .and_then(|v| Some(FnArg::Variable(v)))
                 .or_else(||
            match v {
                &edn::Value::Integer(i) => Some(FnArg::EntidOrInteger(i)),
                &edn::Value::Float(f) => Some(FnArg::Constant(NonIntegerConstant::Float(f))),
                _ => unimplemented!(),
            })
    }
}

/// e, a, tx can't be values -- no strings, no floats -- and so
/// they can only be variables, entity IDs, ident keywords, or
/// placeholders.
/// This encoding allows us to represent integers that aren't
/// entity IDs. That'll get filtered out in the context of the
/// database.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PatternNonValuePlace {
    Placeholder,
    Variable(Variable),
    Entid(i64),                       // Will always be +ve. See #190.
    Ident(NamespacedKeyword),
}

impl PatternNonValuePlace {
    // I think we'll want move variants, so let's leave these here for now.
    #[allow(dead_code)]
    fn into_pattern_value_place(self) -> PatternValuePlace {
        match self {
            PatternNonValuePlace::Placeholder => PatternValuePlace::Placeholder,
            PatternNonValuePlace::Variable(x) => PatternValuePlace::Variable(x),
            PatternNonValuePlace::Entid(x)    => PatternValuePlace::EntidOrInteger(x),
            PatternNonValuePlace::Ident(x)    => PatternValuePlace::IdentOrKeyword(x),
        }
    }

    fn to_pattern_value_place(&self) -> PatternValuePlace {
        match *self {
            PatternNonValuePlace::Placeholder     => PatternValuePlace::Placeholder,
            PatternNonValuePlace::Variable(ref x) => PatternValuePlace::Variable(x.clone()),
            PatternNonValuePlace::Entid(x)        => PatternValuePlace::EntidOrInteger(x),
            PatternNonValuePlace::Ident(ref x)    => PatternValuePlace::IdentOrKeyword(x.clone()),
        }
    }
}

impl FromValue<PatternNonValuePlace> for PatternNonValuePlace {
    fn from_value(v: &edn::Value) -> Option<PatternNonValuePlace> {
        match v {
            &edn::Value::Integer(x) => if x >= 0 {
                Some(PatternNonValuePlace::Entid(x))
            } else {
                None
            },
            &edn::Value::PlainSymbol(ref x) => if x.0.as_str() == "_" {
                Some(PatternNonValuePlace::Placeholder)
            } else {
                if let Some(v) = Variable::from_symbol(x) {
                    Some(PatternNonValuePlace::Variable(v))
                } else {
                    None
                }
            },
            &edn::Value::NamespacedKeyword(ref x) =>
                Some(PatternNonValuePlace::Ident(x.clone())),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IdentOrEntid {
    Ident(NamespacedKeyword),
    Entid(i64),
}

/// The `v` part of a pattern can be much broader: it can represent
/// integers that aren't entity IDs (particularly negative integers),
/// strings, and all the rest. We group those under `Constant`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PatternValuePlace {
    Placeholder,
    Variable(Variable),
    EntidOrInteger(i64),
    IdentOrKeyword(NamespacedKeyword),
    Constant(NonIntegerConstant),
}

impl FromValue<PatternValuePlace> for PatternValuePlace {
    fn from_value(v: &edn::Value) -> Option<PatternValuePlace> {
        match v {
            &edn::Value::Integer(x) =>
                Some(PatternValuePlace::EntidOrInteger(x)),
            &edn::Value::PlainSymbol(ref x) if x.0.as_str() == "_" =>
                Some(PatternValuePlace::Placeholder),
            &edn::Value::PlainSymbol(ref x) if x.is_var_symbol() =>
                Some(PatternValuePlace::Variable(Variable(x.clone()))),
            &edn::Value::NamespacedKeyword(ref x) =>
                Some(PatternValuePlace::IdentOrKeyword(x.clone())),
            &edn::Value::Boolean(x) =>
                Some(PatternValuePlace::Constant(NonIntegerConstant::Boolean(x))),
            &edn::Value::Float(x) =>
                Some(PatternValuePlace::Constant(NonIntegerConstant::Float(x))),
            &edn::Value::BigInteger(ref x) =>
                Some(PatternValuePlace::Constant(NonIntegerConstant::BigInteger(x.clone()))),
            &edn::Value::Text(ref x) =>
                Some(PatternValuePlace::Constant(NonIntegerConstant::Text(x.clone()))),
            _ => None,
        }
    }
}

impl PatternValuePlace {
    // I think we'll want move variants, so let's leave these here for now.
    #[allow(dead_code)]
    fn into_pattern_non_value_place(self) -> Option<PatternNonValuePlace> {
        match self {
            PatternValuePlace::Placeholder       => Some(PatternNonValuePlace::Placeholder),
            PatternValuePlace::Variable(x)       => Some(PatternNonValuePlace::Variable(x)),
            PatternValuePlace::EntidOrInteger(x) => if x >= 0 {
                Some(PatternNonValuePlace::Entid(x))
            } else {
                None
            },
            PatternValuePlace::IdentOrKeyword(x) => Some(PatternNonValuePlace::Ident(x)),
            PatternValuePlace::Constant(_)       => None,
        }
    }

    fn to_pattern_non_value_place(&self) -> Option<PatternNonValuePlace> {
        match *self {
            PatternValuePlace::Placeholder           => Some(PatternNonValuePlace::Placeholder),
            PatternValuePlace::Variable(ref x)       => Some(PatternNonValuePlace::Variable(x.clone())),
            PatternValuePlace::EntidOrInteger(x)     => if x >= 0 {
                Some(PatternNonValuePlace::Entid(x))
            } else {
                None
            },
            PatternValuePlace::IdentOrKeyword(ref x) => Some(PatternNonValuePlace::Ident(x.clone())),
            PatternValuePlace::Constant(_)           => None,
        }
    }
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

#[derive(Clone,Debug,Eq,PartialEq)]
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
#[derive(Clone,Debug,Eq,PartialEq)]
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
impl FindSpec {
    pub fn is_unit_limited(&self) -> bool {
        use FindSpec::*;
        match self {
            &FindScalar(..) => true,
            &FindTuple(..)  => true,
            &FindRel(..)    => false,
            &FindColl(..)   => false,
        }
    }

    pub fn expected_column_count(&self) -> usize {
        use FindSpec::*;
        match self {
            &FindScalar(..) => 1,
            &FindColl(..)   => 1,
            &FindTuple(ref elems) | &FindRel(ref elems) => elems.len(),
        }
    }


    /// Returns true if the provided `FindSpec` cares about distinct results.
    ///
    /// I use the words "cares about" because find is generally defined in terms of producing distinct
    /// results at the Datalog level.
    ///
    /// Two of the find specs (scalar and tuple) produce only a single result. Those don't need to be
    /// run with `SELECT DISTINCT`, because we're only consuming a single result. Those queries will be
    /// run with `LIMIT 1`.
    ///
    /// Additionally, some projections cannot produce duplicate results: `[:find (max ?x) …]`, for
    /// example.
    ///
    /// This function gives us the hook to add that logic when we're ready.
    ///
    /// Beyond this, `DISTINCT` is not always needed. For example, in some kinds of accumulation or
    /// sampling projections we might not need to do it at the SQL level because we're consuming into
    /// a dupe-eliminating data structure like a Set, or we know that a particular query cannot produce
    /// duplicate results.
    pub fn requires_distinct(&self) -> bool {
        !self.is_unit_limited()
    }
}

// Note that the "implicit blank" rule applies.
// A pattern with a reversed attribute — :foo/_bar — is reversed
// at the point of parsing. These `Pattern` instances only represent
// one direction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Pattern {
    pub source: Option<SrcVar>,
    pub entity: PatternNonValuePlace,
    pub attribute: PatternNonValuePlace,
    pub value: PatternValuePlace,
    pub tx: PatternNonValuePlace,
}

impl Pattern {
    pub fn new(src: Option<SrcVar>,
               e: PatternNonValuePlace,
               a: PatternNonValuePlace,
               v: PatternValuePlace,
               tx: PatternNonValuePlace) -> Option<Pattern> {
        let aa = a.clone();       // Too tired of fighting borrow scope for now.
        if let PatternNonValuePlace::Ident(ref k) = aa {
            if k.is_backward() {
                // e and v have different types; we must convert them.
                // Not every parseable value is suitable for the entity field!
                // As such, this is a failable constructor.
                let e_v = e.to_pattern_value_place();
                if let Some(v_e) = v.to_pattern_non_value_place() {
                    return Some(Pattern {
                        source: src,
                        entity: v_e,
                        attribute: PatternNonValuePlace::Ident(k.to_reversed()),
                        value: e_v,
                        tx: tx,
                    });
                } else {
                    return None;
                }
            }
        }
        Some(Pattern {
            source: src,
            entity: e,
            attribute: a,
            value: v,
            tx: tx,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Predicate {
    pub operator: PlainSymbol,
    pub args: Vec<FnArg>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UnifyVars {
    Implicit,
    Explicit(Vec<Variable>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OrWhereClause {
    Clause(WhereClause),
    And(Vec<WhereClause>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrJoin {
    pub unify_vars: UnifyVars,
    pub clauses: Vec<OrWhereClause>,
}

#[allow(dead_code)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WhereClause {
    Not,
    NotJoin,
    OrJoin(OrJoin),
    Pred(Predicate),
    WhereFn,
    RuleExpr,
    Pattern(Pattern),
}

#[allow(dead_code)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FindQuery {
    pub find_spec: FindSpec,
    pub default_source: SrcVar,
    pub with: Vec<Variable>,
    pub in_vars: Vec<Variable>,
    pub in_sources: Vec<SrcVar>,
    pub where_clauses: Vec<WhereClause>,
    // TODO: in_rules;
}
