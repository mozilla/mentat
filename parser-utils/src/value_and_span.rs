// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std;
use std::fmt::{Display, Formatter};
use std::cmp::Ordering;

use combine::{
    ConsumedResult,
    ParseError,
    Parser,
    StreamOnce,
    eof,
    satisfy,
    satisfy_map,
};
use combine::primitives;
use combine::primitives::{
    FastResult,
};
use combine::combinator::{
    Eof,
    Skip,
};

use edn;

/// A wrapper to let us order `edn::Span` in whatever way is appropriate for parsing with `combine`.
#[derive(Clone, Debug)]
pub struct SpanPosition(edn::Span);

impl Display for SpanPosition {
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl PartialEq for SpanPosition {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for SpanPosition { }

impl PartialOrd for SpanPosition {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SpanPosition {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.0).0.cmp(&(other.0).0)
    }
}

/// An iterator specifically for iterating `edn::ValueAndSpan` instances in various ways.
///
/// Enumerating each iteration type allows us to have a single `combine::Stream` implementation
/// yielding `ValueAndSpan` items, which allows us to yield uniform `combine::ParseError` types from
/// disparate parsers.
#[derive(Clone)]
pub enum IntoIter {
    Empty(std::iter::Empty<edn::ValueAndSpan>),
    Atom(std::iter::Once<edn::ValueAndSpan>),
    Vector(std::vec::IntoIter<edn::ValueAndSpan>),
    List(std::collections::linked_list::IntoIter<edn::ValueAndSpan>),
    /// Iterates via a single `flat_map` [k1, v1, k2, v2, ...].
    Map(std::vec::IntoIter<edn::ValueAndSpan>),
    // TODO: Support Set and Map more naturally.  This is significantly more work because the
    // existing BTreeSet and BTreeMap iterators do not implement Clone, and implementing Clone for
    // them is involved.  Since we don't really need to parse sets and maps at this time, this will
    // do for now.
}

impl Iterator for IntoIter {
    type Item = edn::ValueAndSpan;

    fn next(&mut self) -> Option<Self::Item> {
        match *self {
            IntoIter::Empty(ref mut i) => i.next(),
            IntoIter::Atom(ref mut i) => i.next(),
            IntoIter::Vector(ref mut i) => i.next(),
            IntoIter::List(ref mut i) => i.next(),
            IntoIter::Map(ref mut i) => i.next(),
        }
    }
}

/// A single `combine::Stream` implementation iterating `edn::ValueAndSpan` instances.  Equivalent
/// to `combine::IteratorStream` as produced by `combine::from_iter`, but specialized to
/// `edn::ValueAndSpan`.
#[derive(Clone)]
pub struct Stream(IntoIter, SpanPosition);

/// Things specific to parsing with `combine` and our `Stream` that need a trait to live outside of
/// the `edn` crate.
pub trait Item: Clone + PartialEq + Sized {
    /// Position could be specialized to `SpanPosition`.
    type Position: Clone + Ord + std::fmt::Display;

    /// A slight generalization of `combine::Positioner` that allows to set the position based on
    /// the `edn::ValueAndSpan` being iterated.
    fn start(&self) -> Self::Position;
    fn update_position(&self, &mut Self::Position);

    fn into_child_stream_iter(self) -> IntoIter;
    fn into_child_stream(self) -> Stream;
    fn into_atom_stream_iter(self) -> IntoIter;
    fn into_atom_stream(self) -> Stream;
}

impl Item for edn::ValueAndSpan {
    type Position = SpanPosition;

    fn start(&self) -> Self::Position {
        SpanPosition(self.span.clone())
    }

    fn update_position(&self, position: &mut Self::Position) {
        *position = SpanPosition(self.span.clone())
    }

    fn into_child_stream_iter(self) -> IntoIter {
        match self.inner {
            edn::SpannedValue::Vector(values) => IntoIter::Vector(values.into_iter()),
            edn::SpannedValue::List(values) => IntoIter::List(values.into_iter()),
            // Parsing pairs with `combine` is tricky; parsing sequences is easy.
            edn::SpannedValue::Map(map) => IntoIter::Map(map.into_iter().flat_map(|(a, v)| std::iter::once(a).chain(std::iter::once(v))).collect::<Vec<_>>().into_iter()),
            _ => IntoIter::Empty(std::iter::empty()),
        }
    }

    fn into_child_stream(self) -> Stream {
        let span = self.span.clone();
        Stream(self.into_child_stream_iter(), SpanPosition(span))
    }

    fn into_atom_stream_iter(self) -> IntoIter {
        IntoIter::Atom(std::iter::once(self))
    }

    fn into_atom_stream(self) -> Stream {
        let span = self.span.clone();
        Stream(self.into_atom_stream_iter(), SpanPosition(span))
    }
}

/// `Of` and `of` allow us to express nested parsers naturally.
///
/// For example, `vector().of(many(list()))` parses a vector-of-lists, like [(1 2) (:a :b) ("test") ()].
///
/// The "outer" parser `P` and the "nested" parser `N` must be compatible: `P` must produce an
/// output `edn::ValueAndSpan` which can itself be turned into a stream of child elements; and `N`
/// must accept the resulting input `Stream`.  This compatibility allows us to lift errors from the
/// nested parser to the outer parser, which is part of what has made parsing `&'a [edn::Value]`
/// difficult.
#[derive(Clone)]
pub struct Of<P, N>(P, N);

impl<P, N, O> Parser for Of<P, N>
    where P: Parser<Input=Stream, Output=edn::ValueAndSpan>,
          N: Parser<Input=Stream, Output=O>,
{
    type Input = P::Input;
    type Output = O;
    #[inline]
    fn parse_lazy(&mut self, input: Self::Input) -> ConsumedResult<Self::Output, Self::Input> {
        use self::FastResult::*;

        match self.0.parse_lazy(input) {
            ConsumedOk((outer_value, outer_input)) => {
                match self.1.parse_lazy(outer_value.into_child_stream()) {
                    ConsumedOk((inner_value, _)) | EmptyOk((inner_value, _)) => ConsumedOk((inner_value, outer_input)),
                    // TODO: Improve the error output to reference the nested value (or span) in
                    // some way.  This seems surprisingly difficult to do, so we just surface the
                    // inner error message right now.  See also the comment below.
                    EmptyErr(e) | ConsumedErr(e) => ConsumedErr(e),
                }
            },
            EmptyOk((outer_value, outer_input)) => {
                match self.1.parse_lazy(outer_value.into_child_stream()) {
                    ConsumedOk((inner_value, _)) | EmptyOk((inner_value, _)) => EmptyOk((inner_value, outer_input)),
                    // TODO: Improve the error output.  See the comment above.
                    EmptyErr(e) | ConsumedErr(e) => EmptyErr(e),
                }
            },
            ConsumedErr(e) => ConsumedErr(e),
            EmptyErr(e) => EmptyErr(e),
        }
    }

    fn add_error(&mut self, errors: &mut ParseError<Self::Input>) {
        self.0.add_error(errors);
    }
}

#[inline(always)]
pub fn of<P, N, O>(p: P, n: N) -> Of<P, Skip<N, Eof<Stream>>>
    where P: Parser<Input=Stream, Output=edn::ValueAndSpan>,
          N: Parser<Input=Stream, Output=O>,
{
    Of(p, n.skip(eof()))
}

/// We need a trait to define `Parser.of` and have it live outside of the `combine` crate.
pub trait OfParsing: Parser + Sized {
    fn of<N, O>(self, n: N) -> Of<Self, Skip<N, Eof<Self::Input>>>
        where Self: Sized,
              N: Parser<Input = Self::Input, Output=O>;
}

impl<P> OfParsing for P
    where P: Parser<Input=Stream, Output=edn::ValueAndSpan>
{
    fn of<N, O>(self, n: N) -> Of<P, Skip<N, Eof<Self::Input>>>
        where N: Parser<Input = Self::Input, Output=O>
    {
        of(self, n)
    }
}

/// Equivalent to `combine::IteratorStream`.
impl StreamOnce for Stream
{
    type Item = edn::ValueAndSpan;
    type Range = edn::ValueAndSpan;
    type Position = SpanPosition;

    #[inline]
    fn uncons(&mut self) -> std::result::Result<Self::Item, primitives::Error<Self::Item, Self::Item>> {
        match self.0.next() {
            Some(x) => {
                x.update_position(&mut self.1);
                Ok(x)
            },
            None => Err(primitives::Error::end_of_input()),
        }
    }

    #[inline(always)]
    fn position(&self) -> Self::Position {
        self.1.clone()
    }
}

/// Shorthands, just enough to convert the `mentat_db` crate for now.  Written using `Box` for now:
/// it's simple and we can address allocation issues if and when they surface.
pub fn vector() -> Box<Parser<Input=Stream, Output=edn::ValueAndSpan>> {
    satisfy(|v: edn::ValueAndSpan| v.inner.is_vector()).boxed()
}

pub fn list() -> Box<Parser<Input=Stream, Output=edn::ValueAndSpan>> {
    satisfy(|v: edn::ValueAndSpan| v.inner.is_list()).boxed()
}

pub fn map() -> Box<Parser<Input=Stream, Output=edn::ValueAndSpan>> {
    satisfy(|v: edn::ValueAndSpan| v.inner.is_map()).boxed()
}

pub fn integer() -> Box<Parser<Input=Stream, Output=i64>> {
    satisfy_map(|v: edn::ValueAndSpan| v.inner.as_integer()).boxed()
}

pub fn namespaced_keyword() -> Box<Parser<Input=Stream, Output=edn::NamespacedKeyword>> {
    satisfy_map(|v: edn::ValueAndSpan| v.inner.as_namespaced_keyword().cloned()).boxed()
}

/// Like `combine::token()`, but compare an `edn::Value` to an `edn::ValueAndSpan`.
pub fn value(value: edn::Value) -> Box<Parser<Input=Stream, Output=edn::ValueAndSpan>> {
    // TODO: make this comparison faster.  Right now, we drop all the spans; if we walked the value
    // trees together, we could avoid creating garbage.
    satisfy(move |v: edn::ValueAndSpan| value == v.inner.into()).boxed()
}
