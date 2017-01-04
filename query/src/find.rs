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


// TODO: support other kinds of sources.
#[derive(Clone,Debug,Eq,PartialEq)]
pub enum SrcVar {
    DefaultSrc,
}

pub enum Constant {}    // This is essentially Box. TODO: flesh out.

#[derive(Clone,Debug,Eq,PartialEq)]
pub struct Variable {
    pub name: String,
}

pub enum FnArg {
    Constant { constant: Constant },
    Variable { variable: Variable },
    Src      { src: SrcVar },
}

pub enum PullPattern {
    Constant { constant: Constant },
    Variable { variable: Variable },
}

pub struct Pull {
    pub src: SrcVar,
    pub var: Variable,
    pub pattern: PullPattern,      // Constant, variable, or plain variable.
}

pub struct Aggregate {
    pub fn_name: String,
    pub args: Vec<FnArg>,
}

// TODO: look up the idiomatic way to express these kinds of type
// combinations in Rust. It must be common in ASTs. Trait objects
// presumably aren't the answer…
pub enum Element {
    Variable { variable: Variable },
    Pull { expression: Pull },
    Aggregate { expression: Aggregate },
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
/// # use mentat_query::find::{FindSpec, Element, Variable};
///
/// // TODO: this feels clunky.
/// let foo = Variable { name: "foo".to_string() };
/// let bar = Variable { name: "bar".to_string() };
/// let elements = vec![
///   Element::Variable { variable: foo },
///   Element::Variable { variable: bar },
/// ];
/// let rel = FindSpec::FindRel { elements: elements };
///
/// if let FindSpec::FindRel { elements } = rel {
///   assert_eq!(2, elements.len());
/// }
/// ```
///
pub enum FindSpec {
    /// Returns an array of arrays.
    FindRel { elements: Vec<Element> },

    /// Returns an array of scalars, usually homogeneous.
    /// This is equivalent to mapping over the results of a `FindRel`,
    /// returning the first value of each.
    FindColl { element: Element },

    /// Returns a single tuple: a heterogeneous array of scalars. Equivalent to
    /// taking the first result from a `FindRel`.
    FindTuple { elements: Vec<Element> },

    /// Returns a single scalar value. Equivalent to taking the first result
    /// from a `FindColl`.
    FindScalar { element: Element },
}

/// Returns true if the provided `FindSpec` returns at most one result.
pub fn is_unit_limited(spec: &FindSpec) -> bool {
    match spec {
        &FindSpec::FindScalar { .. } => true,
        &FindSpec::FindTuple { .. }  => true,
        &FindSpec::FindRel { .. }    => false,
        &FindSpec::FindColl { .. }   => false,
    }
}

/// Returns true if the provided `FindSpec` cares about distinct results.
pub fn requires_distinct(spec: &FindSpec) -> bool {
    return !is_unit_limited(spec);
}
