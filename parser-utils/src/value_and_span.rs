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
use std::fmt::{
    Debug,
    Display,
    Formatter,
};
use std::cmp::Ordering;

use combine::{
    ConsumedResult,
    ParseError,
    Parser,
    ParseResult,
    StreamOnce,
    many,
    many1,
    parser,
    satisfy,
    satisfy_map,
};
use combine::primitives; // To not shadow Error.
use combine::primitives::{
    Consumed,
    FastResult,
};
use combine::combinator::{
    Expected,
    FnParser,
};

use edn;

/// A wrapper to let us order `edn::Span` in whatever way is appropriate for parsing with `combine`.
#[derive(Clone, Copy, Debug)]
pub struct SpanPosition(edn::Span);

impl Display for SpanPosition {
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        self.0.fmt(f)
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

/// `OfExactly` and `of_exactly` allow us to express nested parsers naturally.
///
/// For example, `vector().of_exactly(many(list()))` parses a vector-of-lists, like [(1 2) (:a :b) ("test") ()].
///
/// The "outer" parser `P` and the "nested" parser `N` must be compatible: `P` must produce an
/// output `edn::ValueAndSpan` which can itself be turned into a stream of child elements; and `N`
/// must accept the resulting input `Stream`.  This compatibility allows us to lift errors from the
/// nested parser to the outer parser, which is part of what has made parsing `&'a [edn::Value]`
/// difficult.
#[derive(Clone)]
pub struct OfExactly<P, N>(P, N);

impl<P, N, O> Parser for OfExactly<P, N>
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
                    ConsumedOk((inner_value, mut inner_input)) | EmptyOk((inner_value, mut inner_input)) => {
                        match inner_input.uncons() {
                            Err(ref err) if *err == primitives::Error::end_of_input() => ConsumedOk((inner_value, outer_input)),
                            _ => EmptyErr(ParseError::empty(inner_input.position())),
                        }
                    },
                    // TODO: Improve the error output to reference the nested value (or span) in
                    // some way.  This seems surprisingly difficult to do, so we just surface the
                    // inner error message right now.  See also the comment below.
                    EmptyErr(e) | ConsumedErr(e) => ConsumedErr(e),
                }
            },
            EmptyOk((outer_value, outer_input)) => {
                match self.1.parse_lazy(outer_value.into_child_stream()) {
                    ConsumedOk((inner_value, mut inner_input)) | EmptyOk((inner_value, mut inner_input)) => {
                        match inner_input.uncons() {
                            Err(ref err) if *err == primitives::Error::end_of_input() => EmptyOk((inner_value, outer_input)),
                            _ => EmptyErr(ParseError::empty(inner_input.position())),
                        }
                    },
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
pub fn of_exactly<P, N, O>(p: P, n: N) -> OfExactly<P, N>
    where P: Parser<Input=Stream, Output=edn::ValueAndSpan>,
          N: Parser<Input=Stream, Output=O>,
{
    OfExactly(p, n)
}

/// We need a trait to define `Parser.of` and have it live outside of the `combine` crate.
pub trait OfExactlyParsing: Parser + Sized {
    fn of_exactly<N, O>(self, n: N) -> OfExactly<Self, N>
        where Self: Sized,
              N: Parser<Input = Self::Input, Output=O>;
}

impl<P> OfExactlyParsing for P
    where P: Parser<Input=Stream, Output=edn::ValueAndSpan>
{
    fn of_exactly<N, O>(self, n: N) -> OfExactly<P, N>
        where N: Parser<Input = Self::Input, Output=O>
    {
        of_exactly(self, n)
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

pub fn seq() -> Box<Parser<Input=Stream, Output=edn::ValueAndSpan>> {
    satisfy(|v: edn::ValueAndSpan| v.inner.is_list() || v.inner.is_vector()).boxed()
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

fn keyword_map_(input: Stream) -> ParseResult<edn::ValueAndSpan, Stream>
{
    // One run is a keyword followed by one or more non-keywords.
    let run = (satisfy(|v: edn::ValueAndSpan| v.inner.is_keyword()),
               many1(satisfy(|v: edn::ValueAndSpan| !v.inner.is_keyword()))
               .map(|vs: Vec<edn::ValueAndSpan>| {
                   // TODO: extract "spanning".
                   let beg = vs.first().unwrap().span.0;
                   let end = vs.last().unwrap().span.1;
                   edn::ValueAndSpan {
                       inner: edn::SpannedValue::Vector(vs),
                       span: edn::Span(beg, end),
                   }
               }));

    let mut runs = vector().of_exactly(many::<Vec<_>, _>(run));

    let (data, input) = try!(runs.parse_lazy(input).into());

    let mut m: std::collections::BTreeMap<edn::ValueAndSpan, edn::ValueAndSpan> = std::collections::BTreeMap::default();
    for (k, vs) in data {
        if m.insert(k, vs).is_some() {
            // TODO: improve this message.
            return Err(Consumed::Empty(ParseError::from_errors(input.into_inner().position(), Vec::new())))
        }
    }

    let map = edn::ValueAndSpan {
        inner: edn::SpannedValue::Map(m),
        span: edn::Span(0, 0), // TODO: fix this.
    };

    Ok((map, input))
}

/// Turn a vector of keywords and non-keyword values into a map.  As an example, turn
/// ```edn
/// [:keyword1 value1 value2 ... :keyword2 value3 value4 ...]
/// ```
/// into
/// ```edn
/// {:keyword1 [value1 value2 ...] :keyword2 [value3 value4 ...]}
/// ```.
pub fn keyword_map() -> Expected<FnParser<Stream, fn(Stream) -> ParseResult<edn::ValueAndSpan, Stream>>>
{
    // The `as` work arounds https://github.com/rust-lang/rust/issues/20178.
    parser(keyword_map_ as fn(Stream) -> ParseResult<edn::ValueAndSpan, Stream>).expected("keyword map")
}

/// Generate a `satisfy` expression that matches a `PlainSymbol` value with the given name.
///
/// We do this rather than using `combine::token` so that we don't need to allocate a new `String`
/// inside a `PlainSymbol` inside a `SpannedValue` inside a `ValueAndSpan` just to match input.
#[macro_export]
macro_rules! def_matches_plain_symbol {
    ( $parser: ident, $name: ident, $input: expr ) => {
        def_parser!($parser, $name, edn::ValueAndSpan, {
            satisfy(|v: edn::ValueAndSpan| {
                match v.inner {
                    edn::SpannedValue::PlainSymbol(ref s) => s.0.as_str() == $input,
                    _ => false,
                }
            })
        });
    }
}

/// Generate a `satisfy` expression that matches a `Keyword` value with the given name.
///
/// We do this rather than using `combine::token` to save allocations.
#[macro_export]
macro_rules! def_matches_keyword {
    ( $parser: ident, $name: ident, $input: expr ) => {
        def_parser!($parser, $name, edn::ValueAndSpan, {
            satisfy(|v: edn::ValueAndSpan| {
                match v.inner {
                    edn::SpannedValue::Keyword(ref s) => s.0.as_str() == $input,
                    _ => false,
                }
            })
        });
    }
}

/// Generate a `satisfy` expression that matches a `NamespacedKeyword` value with the given
/// namespace and name.
///
/// We do this rather than using `combine::token` to save allocations.
#[macro_export]
macro_rules! def_matches_namespaced_keyword {
    ( $parser: ident, $name: ident, $input_namespace: expr, $input_name: expr ) => {
        def_parser!($parser, $name, edn::ValueAndSpan, {
            satisfy(|v: edn::ValueAndSpan| {
                match v.inner {
                    edn::SpannedValue::NamespacedKeyword(ref s) => s.namespace.as_str() == $input_namespace && s.name.as_str() == $input_name,
                    _ => false,
                }
            })
        });
    }
}

#[cfg(test)]
mod tests {
    use combine::{eof};
    use super::*;

    /// Take a string `input` and a string `expected` and ensure that `input` parses to an
    /// `edn::Value` keyword map equivalent to the `edn::Value` that `expected` parses to.
    macro_rules! assert_keyword_map_eq {
        ( $input: expr, $expected: expr ) => {{
            let input = edn::parse::value($input).expect("to be able to parse input EDN");
            let expected = $expected.map(|e| {
                edn::parse::value(e).expect("to be able to parse expected EDN").without_spans()
            });
            let mut par = keyword_map().map(|x| x.without_spans()).skip(eof());
            let result = par.parse(input.into_atom_stream()).map(|x| x.0);
            assert_eq!(result.ok(), expected);
        }}
    }

    #[test]
    fn test_keyword_map() {
        assert_keyword_map_eq!(
            "[:foo 1 2 3 :bar 4]",
            Some("{:foo [1 2 3] :bar [4]}"));

        // Trailing keywords aren't allowed.
        assert_keyword_map_eq!(
            "[:foo]",
            None);
        assert_keyword_map_eq!(
            "[:foo 2 :bar]",
            None);

        // Duplicate keywords aren't allowed.
        assert_keyword_map_eq!(
            "[:foo 2 :foo 1]",
            None);

        // Starting with anything but a keyword isn't allowed.
        assert_keyword_map_eq!(
            "[2 :foo 1]",
            None);

        // Consecutive keywords aren't allowed.
        assert_keyword_map_eq!(
            "[:foo :bar 1]",
            None);

        // Empty lists return an empty map.
        assert_keyword_map_eq!(
            "[]",
            Some("{}"));
    }
}
