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

use std::collections::{
    BTreeSet,
    HashSet,
};

use std::fmt;
use std::rc::Rc;

use edn::{
    BigInt,
    DateTime,
    OrderedFloat,
    Uuid,
    UTC,
};

pub use edn::{
    NamespacedKeyword,
    PlainSymbol,
};

use mentat_core::{
    TypedValue,
};

pub type SrcVarName = String;          // Do not include the required syntactic '$'.

#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Variable(pub Rc<PlainSymbol>);

impl Variable {
    pub fn as_str(&self) -> &str {
        self.0.as_ref().0.as_str()
    }

    pub fn to_string(&self) -> String {
        self.0.as_ref().0.clone()
    }

    pub fn name(&self) -> PlainSymbol {
        self.0.as_ref().clone()
    }

    /// Return a new `Variable`, assuming that the provided string is a valid name.
    pub fn from_valid_name(name: &str) -> Variable {
        let s = PlainSymbol::new(name);
        assert!(s.is_var_symbol());
        Variable(Rc::new(s))
    }
}

pub trait FromValue<T> {
    fn from_value(v: &edn::ValueAndSpan) -> Option<T>;
}

/// If the provided EDN value is a PlainSymbol beginning with '?', return
/// it wrapped in a Variable. If not, return None.
/// TODO: intern strings. #398.
impl FromValue<Variable> for Variable {
    fn from_value(v: &edn::ValueAndSpan) -> Option<Variable> {
        if let edn::SpannedValue::PlainSymbol(ref s) = v.inner {
            Variable::from_symbol(s)
        } else {
            None
        }
    }
}

impl Variable {
    pub fn from_rc(sym: Rc<PlainSymbol>) -> Option<Variable> {
        if sym.is_var_symbol() {
            Some(Variable(sym.clone()))
        } else {
            None
        }
    }

    /// TODO: intern strings. #398.
    pub fn from_symbol(sym: &PlainSymbol) -> Option<Variable> {
        if sym.is_var_symbol() {
            Some(Variable(Rc::new(sym.clone())))
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

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct QueryFunction(pub PlainSymbol);

impl FromValue<QueryFunction> for QueryFunction {
    fn from_value(v: &edn::ValueAndSpan) -> Option<QueryFunction> {
        if let edn::SpannedValue::PlainSymbol(ref s) = v.inner {
            QueryFunction::from_symbol(s)
        } else {
            None
        }
    }
}

impl QueryFunction {
    pub fn from_symbol(sym: &PlainSymbol) -> Option<QueryFunction> {
        // TODO: validate the acceptable set of function names.
        Some(QueryFunction(sym.clone()))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Direction {
    Ascending,
    Descending,
}

/// An abstract declaration of ordering: direction and variable.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Order(pub Direction, pub Variable);   // Future: Element instead of Variable?

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum SrcVar {
    DefaultSrc,
    NamedSrc(SrcVarName),
}

impl FromValue<SrcVar> for SrcVar {
    fn from_value(v: &edn::ValueAndSpan) -> Option<SrcVar> {
        if let edn::SpannedValue::PlainSymbol(ref s) = v.inner {
            SrcVar::from_symbol(s)
        } else {
            None
        }
    }
}

impl SrcVar {
    pub fn from_symbol(sym: &PlainSymbol) -> Option<SrcVar> {
        if sym.is_src_symbol() {
            if sym.0 == "$" {
                Some(SrcVar::DefaultSrc)
            } else {
                Some(SrcVar::NamedSrc(sym.plain_name().to_string()))
            }
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
    Text(Rc<String>),
    Instant(DateTime<UTC>),
    Uuid(Uuid),
}

impl NonIntegerConstant {
    pub fn into_typed_value(self) -> TypedValue {
        match self {
            NonIntegerConstant::BigInteger(_) => unimplemented!(),     // TODO: #280.
            NonIntegerConstant::Boolean(v) => TypedValue::Boolean(v),
            NonIntegerConstant::Float(v) => TypedValue::Double(v),
            NonIntegerConstant::Text(v) => TypedValue::String(v),
            NonIntegerConstant::Instant(v) => TypedValue::Instant(v),
            NonIntegerConstant::Uuid(v) => TypedValue::Uuid(v),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FnArg {
    Variable(Variable),
    SrcVar(SrcVar),
    EntidOrInteger(i64),
    IdentOrKeyword(NamespacedKeyword),
    Constant(NonIntegerConstant),
    // The collection values representable in EDN.  There's no advantage to destructuring up front,
    // since consumers will need to handle arbitrarily nested EDN themselves anyway.
    Vector(Vec<FnArg>),
}

impl FromValue<FnArg> for FnArg {
    fn from_value(v: &edn::ValueAndSpan) -> Option<FnArg> {
        use edn::SpannedValue::*;
        match v.inner {
            Integer(x) =>
                Some(FnArg::EntidOrInteger(x)),
            PlainSymbol(ref x) if x.is_src_symbol() =>
                SrcVar::from_symbol(x).map(FnArg::SrcVar),
            PlainSymbol(ref x) if x.is_var_symbol() =>
                Variable::from_symbol(x).map(FnArg::Variable),
            PlainSymbol(_) => None,
            NamespacedKeyword(ref x) =>
                Some(FnArg::IdentOrKeyword(x.clone())),
            Instant(x) =>
                Some(FnArg::Constant(NonIntegerConstant::Instant(x))),
            Uuid(x) =>
                Some(FnArg::Constant(NonIntegerConstant::Uuid(x))),
            Boolean(x) =>
                Some(FnArg::Constant(NonIntegerConstant::Boolean(x))),
            Float(x) =>
                Some(FnArg::Constant(NonIntegerConstant::Float(x))),
            BigInteger(ref x) =>
                Some(FnArg::Constant(NonIntegerConstant::BigInteger(x.clone()))),
            Text(ref x) =>
                // TODO: intern strings. #398.
                Some(FnArg::Constant(NonIntegerConstant::Text(Rc::new(x.clone())))),
            Nil |
            NamespacedSymbol(_) |
            Keyword(_) |
            Vector(_) |
            List(_) |
            Set(_) |
            Map(_) => None,
        }
    }
}

impl FnArg {
    pub fn as_variable(&self) -> Option<&Variable> {
        match self {
            &FnArg::Variable(ref v) => Some(v),
            _ => None,
        }
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
    Ident(Rc<NamespacedKeyword>),
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
    fn from_value(v: &edn::ValueAndSpan) -> Option<PatternNonValuePlace> {
        match v.inner {
            edn::SpannedValue::Integer(x) => if x >= 0 {
                Some(PatternNonValuePlace::Entid(x))
            } else {
                None
            },
            edn::SpannedValue::PlainSymbol(ref x) => if x.0.as_str() == "_" {
                Some(PatternNonValuePlace::Placeholder)
            } else {
                if let Some(v) = Variable::from_symbol(x) {
                    Some(PatternNonValuePlace::Variable(v))
                } else {
                    None
                }
            },
            edn::SpannedValue::NamespacedKeyword(ref x) =>
                Some(PatternNonValuePlace::Ident(Rc::new(x.clone()))),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IdentOrEntid {
    Ident(Rc<NamespacedKeyword>),
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
    IdentOrKeyword(Rc<NamespacedKeyword>),
    Constant(NonIntegerConstant),
}

impl FromValue<PatternValuePlace> for PatternValuePlace {
    fn from_value(v: &edn::ValueAndSpan) -> Option<PatternValuePlace> {
        match v.inner {
            edn::SpannedValue::Integer(x) =>
                Some(PatternValuePlace::EntidOrInteger(x)),
            edn::SpannedValue::PlainSymbol(ref x) if x.0.as_str() == "_" =>
                Some(PatternValuePlace::Placeholder),
            edn::SpannedValue::PlainSymbol(ref x) =>
                Variable::from_symbol(x).map(PatternValuePlace::Variable),
            edn::SpannedValue::NamespacedKeyword(ref x) =>
                Some(PatternValuePlace::IdentOrKeyword(Rc::new(x.clone()))),
            edn::SpannedValue::Boolean(x) =>
                Some(PatternValuePlace::Constant(NonIntegerConstant::Boolean(x))),
            edn::SpannedValue::Float(x) =>
                Some(PatternValuePlace::Constant(NonIntegerConstant::Float(x))),
            edn::SpannedValue::BigInteger(ref x) =>
                Some(PatternValuePlace::Constant(NonIntegerConstant::BigInteger(x.clone()))),
            edn::SpannedValue::Instant(x) =>
                Some(PatternValuePlace::Constant(NonIntegerConstant::Instant(x))),
            edn::SpannedValue::Text(ref x) =>
                // TODO: intern strings. #398.
                Some(PatternValuePlace::Constant(NonIntegerConstant::Text(Rc::new(x.clone())))),
            edn::SpannedValue::Uuid(ref u) =>
                Some(PatternValuePlace::Constant(NonIntegerConstant::Uuid(u.clone()))),

            // These don't appear in queries.
            edn::SpannedValue::Nil => None,
            edn::SpannedValue::NamespacedSymbol(_) => None,
            edn::SpannedValue::Keyword(_) => None,
            edn::SpannedValue::Map(_) => None,
            edn::SpannedValue::List(_) => None,
            edn::SpannedValue::Set(_) => None,
            edn::SpannedValue::Vector(_) => None,
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

#[derive(Debug, Eq, PartialEq)]
pub struct Aggregate {
    pub func: QueryFunction,
    pub args: Vec<FnArg>,
}

#[derive(Debug, Eq, PartialEq)]
pub enum Element {
    Variable(Variable),
    Aggregate(Aggregate),
    // Pull(Pull),             // TODO
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Limit {
    None,
    Fixed(u64),
    Variable(Variable),
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
/// # use std::rc::Rc;
/// # use edn::PlainSymbol;
/// # use mentat_query::{Element, FindSpec, Variable};
///
/// # fn main() {
///
///   let elements = vec![
///     Element::Variable(Variable::from_valid_name("?foo")),
///     Element::Variable(Variable::from_valid_name("?bar")),
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
#[derive(Debug, Eq, PartialEq)]
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

// Datomic accepts variable or placeholder.  DataScript accepts recursive bindings.  Mentat sticks
// to the non-recursive form Datomic accepts, which is much simpler to process.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum VariableOrPlaceholder {
    Placeholder,
    Variable(Variable),
}

impl VariableOrPlaceholder {
    pub fn into_var(self) -> Option<Variable> {
        match self {
            VariableOrPlaceholder::Placeholder => None,
            VariableOrPlaceholder::Variable(var) => Some(var),
        }
    }

    pub fn var(&self) -> Option<&Variable> {
        match self {
            &VariableOrPlaceholder::Placeholder => None,
            &VariableOrPlaceholder::Variable(ref var) => Some(var),
        }
    }
}

#[derive(Clone,Debug,Eq,PartialEq)]
pub enum Binding {
    BindScalar(Variable),
    BindColl(Variable),
    BindRel(Vec<VariableOrPlaceholder>),
    BindTuple(Vec<VariableOrPlaceholder>),
}

impl Binding {
    /// Return each variable or `None`, in order.
    pub fn variables(&self) -> Vec<Option<Variable>> {
        match self {
            &Binding::BindScalar(ref var) | &Binding::BindColl(ref var) => vec![Some(var.clone())],
            &Binding::BindRel(ref vars) | &Binding::BindTuple(ref vars) => vars.iter().map(|x| x.var().cloned()).collect(),
        }
    }

    /// Return `true` if no variables are bound, i.e., all binding entries are placeholders.
    pub fn is_empty(&self) -> bool {
        match self {
            &Binding::BindScalar(_) | &Binding::BindColl(_) => false,
            &Binding::BindRel(ref vars) | &Binding::BindTuple(ref vars) => vars.iter().all(|x| x.var().is_none()),
        }
    }

    /// Return `true` if no variable is bound twice, i.e., each binding entry is either a
    /// placeholder or unique.
    ///
    /// ```
    /// extern crate mentat_query;
    /// use std::rc::Rc;
    ///
    /// let v = mentat_query::Variable::from_valid_name("?foo");
    /// let vv = mentat_query::VariableOrPlaceholder::Variable(v);
    /// let p = mentat_query::VariableOrPlaceholder::Placeholder;
    ///
    /// let e = mentat_query::Binding::BindTuple(vec![p.clone()]);
    /// let b = mentat_query::Binding::BindTuple(vec![p.clone(), vv.clone()]);
    /// let d = mentat_query::Binding::BindTuple(vec![vv.clone(), p, vv]);
    /// assert!(b.is_valid());          // One var, one placeholder: OK.
    /// assert!(!e.is_valid());         // Empty: not OK.
    /// assert!(!d.is_valid());         // Duplicate var: not OK.
    /// ```
    pub fn is_valid(&self) -> bool {
        match self {
            &Binding::BindScalar(_) | &Binding::BindColl(_) => true,
            &Binding::BindRel(ref vars) | &Binding::BindTuple(ref vars) => {
                let mut acc = HashSet::<Variable>::new();
                for var in vars {
                    if let &VariableOrPlaceholder::Variable(ref var) = var {
                        if !acc.insert(var.clone()) {
                            // It's invalid if there was an equal var already present in the set --
                            // i.e., we have a duplicate var.
                            return false;
                        }
                    }
                }
                // We're not valid if every place is a placeholder!
                !acc.is_empty()
            }
        }
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
                        attribute: PatternNonValuePlace::Ident(Rc::new(k.to_reversed())),
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
pub struct WhereFn {
    pub operator: PlainSymbol,
    pub args: Vec<FnArg>,
    pub binding: Binding,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UnifyVars {
    /// `Implicit` means the variables in an `or` or `not` are derived from the enclosed pattern.
    /// DataScript regards these vars as 'free': these variables don't need to be bound by the
    /// enclosing environment.
    ///
    /// Datomic's documentation implies that all implicit variables are required:
    ///
    /// > Datomic will attempt to push the or clause down until all necessary variables are bound,
    /// > and will throw an exception if that is not possible.
    ///
    /// but that would render top-level `or` expressions (as used in Datomic's own examples!)
    /// impossible, so we assume that this is an error in the documentation.
    ///
    /// All contained 'arms' in an `or` with implicit variables must bind the same vars.
    Implicit,

    /// `Explicit` means the variables in an `or-join` or `not-join` are explicitly listed,
    /// specified with `required-vars` syntax.
    ///
    /// DataScript parses these as free, but allows (incorrectly) the use of more complicated
    /// `rule-vars` syntax.
    ///
    /// Only the named variables will be unified with the enclosing query.
    ///
    /// Every 'arm' in an `or-join` must mention the entire set of explicit vars.
    Explicit(BTreeSet<Variable>),
}

impl WhereClause {
    pub fn is_pattern(&self) -> bool {
        match self {
            &WhereClause::Pattern(_) => true,
            _ => false,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OrWhereClause {
    Clause(WhereClause),
    And(Vec<WhereClause>),
}

impl OrWhereClause {
    pub fn is_pattern_or_patterns(&self) -> bool {
        match self {
            &OrWhereClause::Clause(WhereClause::Pattern(_)) => true,
            &OrWhereClause::And(ref clauses) => clauses.iter().all(|clause| clause.is_pattern()),
            _ => false,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrJoin {
    pub unify_vars: UnifyVars,
    pub clauses: Vec<OrWhereClause>,

    /// Caches the result of `collect_mentioned_variables`.
    mentioned_vars: Option<BTreeSet<Variable>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NotJoin {
    pub unify_vars: UnifyVars,
    pub clauses: Vec<WhereClause>,
}

#[allow(dead_code)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WhereClause {
    NotJoin(NotJoin),
    OrJoin(OrJoin),
    Pred(Predicate),
    WhereFn(WhereFn),
    RuleExpr,
    Pattern(Pattern),
}

#[allow(dead_code)]
#[derive(Debug, Eq, PartialEq)]
pub struct FindQuery {
    pub find_spec: FindSpec,
    pub default_source: SrcVar,
    pub with: BTreeSet<Variable>,
    pub in_vars: BTreeSet<Variable>,
    pub in_sources: BTreeSet<SrcVar>,
    pub limit: Limit,
    pub where_clauses: Vec<WhereClause>,
    pub order: Option<Vec<Order>>,
    // TODO: in_rules;
}

impl OrJoin {
    pub fn new(unify_vars: UnifyVars, clauses: Vec<OrWhereClause>) -> OrJoin {
        OrJoin {
            unify_vars: unify_vars,
            clauses: clauses,
            mentioned_vars: None,
        }
    }

    /// Return true if either the `OrJoin` is `UnifyVars::Implicit`, or if
    /// every variable mentioned inside the join is also mentioned in the `UnifyVars` list.
    pub fn is_fully_unified(&self) -> bool {
        match &self.unify_vars {
            &UnifyVars::Implicit => true,
            &UnifyVars::Explicit(ref vars) => {
                // We know that the join list must be a subset of the vars in the pattern, or
                // it would have failed validation. That allows us to simply compare counts here.
                // TODO: in debug mode, do a full intersection, and verify that our count check
                // returns the same results.
                // Use the cached list if we have one.
                if let Some(ref mentioned) = self.mentioned_vars {
                    vars.len() == mentioned.len()
                } else {
                    vars.len() == self.collect_mentioned_variables().len()
                }
            }
        }
    }
}

pub trait ContainsVariables {
    fn accumulate_mentioned_variables(&self, acc: &mut BTreeSet<Variable>);
    fn collect_mentioned_variables(&self) -> BTreeSet<Variable> {
        let mut out = BTreeSet::new();
        self.accumulate_mentioned_variables(&mut out);
        out
    }
}

impl ContainsVariables for WhereClause {
    fn accumulate_mentioned_variables(&self, acc: &mut BTreeSet<Variable>) {
        use WhereClause::*;
        match self {
            &OrJoin(ref o)  => o.accumulate_mentioned_variables(acc),
            &Pred(ref p)    => p.accumulate_mentioned_variables(acc),
            &Pattern(ref p) => p.accumulate_mentioned_variables(acc),
            &NotJoin(ref n) => n.accumulate_mentioned_variables(acc),
            &WhereFn(ref f) => f.accumulate_mentioned_variables(acc),
            &RuleExpr       => (),
        }
    }
}

impl ContainsVariables for OrWhereClause {
    fn accumulate_mentioned_variables(&self, acc: &mut BTreeSet<Variable>) {
        use OrWhereClause::*;
        match self {
            &And(ref clauses) => for clause in clauses { clause.accumulate_mentioned_variables(acc) },
            &Clause(ref clause) => clause.accumulate_mentioned_variables(acc),
        }
    }
}

impl ContainsVariables for OrJoin {
    fn accumulate_mentioned_variables(&self, acc: &mut BTreeSet<Variable>) {
        for clause in &self.clauses {
            clause.accumulate_mentioned_variables(acc);
        }
    }
}

impl OrJoin {
    pub fn dismember(self) -> (Vec<OrWhereClause>, UnifyVars, BTreeSet<Variable>) {
        let vars = match self.mentioned_vars {
                       Some(m) => m,
                       None => self.collect_mentioned_variables(),
                   };
        (self.clauses, self.unify_vars, vars)
    }

    pub fn mentioned_variables<'a>(&'a mut self) -> &'a BTreeSet<Variable> {
        if self.mentioned_vars.is_none() {
            let m = self.collect_mentioned_variables();
            self.mentioned_vars = Some(m);
        }
        if let Some(ref mentioned) = self.mentioned_vars {
            mentioned
        } else {
            panic!()
        }
    }
}

impl ContainsVariables for NotJoin {
    fn accumulate_mentioned_variables(&self, acc: &mut BTreeSet<Variable>) {
        for clause in &self.clauses {
            clause.accumulate_mentioned_variables(acc);
        }
    }
}

impl ContainsVariables for Predicate {
    fn accumulate_mentioned_variables(&self, acc: &mut BTreeSet<Variable>) {
        for arg in &self.args {
            if let &FnArg::Variable(ref v) = arg {
                acc_ref(acc, v)
            }
        }
    }
}

impl ContainsVariables for Binding {
    fn accumulate_mentioned_variables(&self, acc: &mut BTreeSet<Variable>) {
        match self {
            &Binding::BindScalar(ref v) | &Binding::BindColl(ref v) => {
                acc_ref(acc, v)
            },
            &Binding::BindRel(ref vs) | &Binding::BindTuple(ref vs) => {
                for v in vs {
                    if let &VariableOrPlaceholder::Variable(ref v) = v {
                        acc_ref(acc, v);
                    }
                }
            },
        }
    }
}

impl ContainsVariables for WhereFn {
    fn accumulate_mentioned_variables(&self, acc: &mut BTreeSet<Variable>) {
        for arg in &self.args {
            if let &FnArg::Variable(ref v) = arg {
                acc_ref(acc, v)
            }
        }
        self.binding.accumulate_mentioned_variables(acc);
    }
}

fn acc_ref<T: Clone + Ord>(acc: &mut BTreeSet<T>, v: &T) {
    // Roll on, reference entries!
    if !acc.contains(v) {
        acc.insert(v.clone());
    }
}

impl ContainsVariables for Pattern {
    fn accumulate_mentioned_variables(&self, acc: &mut BTreeSet<Variable>) {
        if let PatternNonValuePlace::Variable(ref v) = self.entity {
            acc_ref(acc, v)
        }
        if let PatternNonValuePlace::Variable(ref v) = self.attribute {
            acc_ref(acc, v)
        }
        if let PatternValuePlace::Variable(ref v) = self.value {
            acc_ref(acc, v)
        }
        if let PatternNonValuePlace::Variable(ref v) = self.tx {
            acc_ref(acc, v)
        }
    }
}
