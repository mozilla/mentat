// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]

use std;
use std::cmp::Ordering;
use std::fmt::{
    Debug,
    Display,
    Formatter,
};

use combine::{
    ConsumedResult,
    ParseError,
    Parser,
    ParseResult,
    StreamOnce,
    parser,
    satisfy_map,
};
use combine::primitives; // To not shadow Error.
use combine::primitives::{
    FastResult,
};
use combine::combinator::{
    Expected,
    FnParser,
};

use edn;

use macros::{
    KeywordMapParser,
};

/// A wrapper to let us order `edn::Span` in whatever way is appropriate for parsing with `combine`.
#[derive(Clone, Copy, Debug)]
pub struct SpanPosition(pub edn::Span);

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
pub enum Iter<'a> {
    Empty,
    Atom(std::iter::Once<&'a edn::ValueAndSpan>),
    Vector(std::slice::Iter<'a, edn::ValueAndSpan>),
    List(std::collections::linked_list::Iter<'a, edn::ValueAndSpan>),
    /// Iterates a map {:k1 v1, :k2 v2, ...} as a single `flat_map` slice [k1, v1, k2, v2, ...].
    Map(std::iter::FlatMap<std::collections::btree_map::Iter<'a, edn::ValueAndSpan, edn::ValueAndSpan>,
                           std::iter::Chain<std::iter::Once<&'a edn::ValueAndSpan>, std::iter::Once<&'a edn::ValueAndSpan>>,
                           fn((&'a edn::ValueAndSpan, &'a edn::ValueAndSpan)) -> std::iter::Chain<std::iter::Once<&'a edn::ValueAndSpan>, std::iter::Once<&'a edn::ValueAndSpan>>>),
    /// Iterates a map with vector values {:k1 [v11 v12 ...], :k2 [v21 v22 ...], ...} as a single
    /// flattened map [k1, v11, v12, ..., k2, v21, v22, ...].
    KeywordMap(std::iter::FlatMap<std::collections::btree_map::Iter<'a, edn::ValueAndSpan, edn::ValueAndSpan>,
                                  std::iter::Chain<std::iter::Once<&'a edn::ValueAndSpan>, Box<Iter<'a>>>,
                                  fn((&'a edn::ValueAndSpan, &'a edn::ValueAndSpan)) -> std::iter::Chain<std::iter::Once<&'a edn::ValueAndSpan>, Box<Iter<'a>>>>),
    // TODO: Support Set and Map more naturally.  This is significantly more work because the
    // existing BTreeSet and BTreeMap iterators do not implement Clone, and implementing Clone for
    // them is involved.  Since we don't really need to parse sets and maps at this time, this will
    // do for now.
}

impl<'a> Iterator for Iter<'a> {
    type Item = &'a edn::ValueAndSpan;

    fn next(&mut self) -> Option<Self::Item> {
        match *self {
            Iter::Empty => None,
            Iter::Atom(ref mut i) => i.next(),
            Iter::Vector(ref mut i) => i.next(),
            Iter::List(ref mut i) => i.next(),
            Iter::Map(ref mut i) => i.next(),
            Iter::KeywordMap(ref mut i) => i.next(),
        }
    }
}

/// A single `combine::Stream` implementation iterating `edn::ValueAndSpan` instances.  Equivalent
/// to `combine::IteratorStream` as produced by `combine::from_iter`, but specialized to
/// `edn::ValueAndSpan`.
#[derive(Clone)]
pub struct Stream<'a>(Iter<'a>, SpanPosition);

/// Things specific to parsing with `combine` and our `Stream` that need a trait to live outside of
/// the `edn` crate.
pub trait Item<'a>: Clone + PartialEq + Sized {
    /// Position could be specialized to `SpanPosition`.
    type Position: Clone + Ord + std::fmt::Display;

    /// A slight generalization of `combine::Positioner` that allows to set the position based on
    /// the `edn::ValueAndSpan` being iterated.
    fn start(&self) -> Self::Position;
    fn update_position(&self, &mut Self::Position);

    fn child_iter(&'a self) -> Iter<'a>;
    fn child_stream(&'a self) -> Stream<'a>;
    fn atom_iter(&'a self) -> Iter<'a>;
    fn atom_stream(&'a self) -> Stream<'a>;

    fn keyword_map_iter(&'a self) -> Iter<'a>;
    fn keyword_map_stream(&'a self) -> Stream<'a>;
}

impl<'a> Item<'a> for edn::ValueAndSpan {
    type Position = SpanPosition;

    fn start(&self) -> Self::Position {
        SpanPosition(self.span.clone())
    }

    fn update_position(&self, position: &mut Self::Position) {
        *position = SpanPosition(self.span.clone())
    }

    fn keyword_map_iter(&'a self) -> Iter<'a> {
        fn flatten_k_vector<'a>((k, v): (&'a edn::ValueAndSpan, &'a edn::ValueAndSpan)) -> std::iter::Chain<std::iter::Once<&'a edn::ValueAndSpan>, Box<Iter<'a>>> {
            std::iter::once(k).chain(Box::new(v.child_iter()))
        }

        match self.inner.as_map() {
            Some(ref map) => Iter::KeywordMap(map.iter().flat_map(flatten_k_vector)),
            None => Iter::Empty
        }
    }

    fn keyword_map_stream(&'a self) -> Stream<'a> {
        let span = self.span.clone();
        Stream(self.keyword_map_iter(), SpanPosition(span))
    }

    fn child_iter(&'a self) -> Iter<'a> {
        fn flatten_k_v<'a>((k, v): (&'a edn::ValueAndSpan, &'a edn::ValueAndSpan)) -> std::iter::Chain<std::iter::Once<&'a edn::ValueAndSpan>, std::iter::Once<&'a edn::ValueAndSpan>> {
            std::iter::once(k).chain(std::iter::once(v))
        }

        match self.inner {
            edn::SpannedValue::Vector(ref values) => Iter::Vector(values.iter()),
            edn::SpannedValue::List(ref values) => Iter::List(values.iter()),
            // Parsing pairs with `combine` is tricky; parsing sequences is easy.
            edn::SpannedValue::Map(ref map) => Iter::Map(map.iter().flat_map(flatten_k_v)),
            _ => Iter::Empty,
        }
    }

    fn child_stream(&'a self) -> Stream<'a> {
        let span = self.span.clone();
        Stream(self.child_iter(), SpanPosition(span))
    }

    fn atom_iter(&'a self) -> Iter<'a> {
        Iter::Atom(std::iter::once(self))
    }

    fn atom_stream(&'a self) -> Stream<'a> {
        let span = self.span.clone();
        Stream(self.atom_iter(), SpanPosition(span))
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

pub trait Streaming<'a> {
    fn as_stream(self) -> Stream<'a>;
}

impl<'a> Streaming<'a> for &'a edn::ValueAndSpan {
    fn as_stream(self) -> Stream<'a> {
        self.child_stream()
    }
}

impl<'a> Streaming<'a> for Stream<'a> {
    fn as_stream(self) -> Stream<'a> {
        self
    }
}

impl<'a, P, N, M, O> Parser for OfExactly<P, N>
    where P: Parser<Input=Stream<'a>, Output=M>,
          N: Parser<Input=Stream<'a>, Output=O>,
          M: 'a + Streaming<'a>,
{
    type Input = P::Input;
    type Output = O;
    #[inline]
    fn parse_lazy(&mut self, input: Self::Input) -> ConsumedResult<Self::Output, Self::Input> {
        use self::FastResult::*;

        match self.0.parse_lazy(input) {
            ConsumedOk((outer_value, outer_input)) => {
                match self.1.parse_lazy(outer_value.as_stream()) {
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
                match self.1.parse_lazy(outer_value.as_stream()) {
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
pub fn of_exactly<'a, P, N, M, O>(p: P, n: N) -> OfExactly<P, N>
    where P: Parser<Input=Stream<'a>, Output=M>,
          N: Parser<Input=Stream<'a>, Output=O>,
          M: 'a + Streaming<'a>,
{
    OfExactly(p, n)
}

/// We need a trait to define `Parser.of` and have it live outside of the `combine` crate.
pub trait OfExactlyParsing: Parser + Sized {
    fn of_exactly<N, O>(self, n: N) -> OfExactly<Self, N>
        where Self: Sized,
              N: Parser<Input = Self::Input, Output=O>;
}

impl<'a, P, M> OfExactlyParsing for P
    where P: Parser<Input=Stream<'a>, Output=M>,
          M: 'a + Streaming<'a>,
{
    fn of_exactly<N, O>(self, n: N) -> OfExactly<P, N>
        where N: Parser<Input = Self::Input, Output=O>
    {
        of_exactly(self, n)
    }
}

/// Equivalent to `combine::IteratorStream`.
impl<'a> StreamOnce for Stream<'a>
{
    type Item = &'a edn::ValueAndSpan;
    type Range = &'a edn::ValueAndSpan;
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
pub fn vector_<'a>(input: Stream<'a>) -> ParseResult<Stream<'a>, Stream<'a>> {
    satisfy_map(|v: &'a edn::ValueAndSpan| {
        if v.inner.is_vector() {
            Some(v.child_stream())
        } else {
            None
        }
    })
        .parse_lazy(input)
        .into()
}

pub fn vector<'a>() -> Expected<FnParser<Stream<'a>, fn(Stream<'a>) -> ParseResult<Stream<'a>, Stream<'a>>>> {
    parser(vector_ as fn(Stream<'a>) -> ParseResult<Stream<'a>, Stream<'a>>).expected("vector")
}

pub fn list_<'a>(input: Stream<'a>) -> ParseResult<Stream<'a>, Stream<'a>> {
    satisfy_map(|v: &'a edn::ValueAndSpan| {
        if v.inner.is_list() {
            Some(v.child_stream())
        } else {
            None
        }
    })
        .parse_lazy(input)
        .into()
}

pub fn list<'a>() -> Expected<FnParser<Stream<'a>, fn(Stream<'a>) -> ParseResult<Stream<'a>, Stream<'a>>>> {
    parser(list_ as fn(Stream<'a>) -> ParseResult<Stream<'a>, Stream<'a>>).expected("list")
}

pub fn seq_<'a>(input: Stream<'a>) -> ParseResult<Stream<'a>, Stream<'a>> {
    satisfy_map(|v: &'a edn::ValueAndSpan| {
        if v.inner.is_list() || v.inner.is_vector() {
            Some(v.child_stream())
        } else {
            None
        }
    })
        .parse_lazy(input)
        .into()
}

pub fn seq<'a>() -> Expected<FnParser<Stream<'a>, fn(Stream<'a>) -> ParseResult<Stream<'a>, Stream<'a>>>> {
    parser(seq_ as fn(Stream<'a>) -> ParseResult<Stream<'a>, Stream<'a>>).expected("vector|list")
}

pub fn map_<'a>(input: Stream<'a>) -> ParseResult<Stream<'a>, Stream<'a>> {
    satisfy_map(|v: &'a edn::ValueAndSpan| {
        if v.inner.is_map() {
            Some(v.child_stream())
        } else {
            None
        }
    })
        .parse_lazy(input)
        .into()
}

pub fn map<'a>() -> Expected<FnParser<Stream<'a>, fn(Stream<'a>) -> ParseResult<Stream<'a>, Stream<'a>>>> {
    parser(map_ as fn(Stream<'a>) -> ParseResult<Stream<'a>, Stream<'a>>).expected("map")
}

/// A `[k v]` pair in the map form of a keyword map must have the shape `[:k, [v1, v2, ...]]`, with
/// none of `v1`, `v2`, ... a keyword: without loss of generality, we cannot represent the case
/// where `vn` is a keyword `:l`, since `[:k v1 v2 ... :l]`, isn't a valid keyword map in vector
/// form.  This function tests that a `[k v]` pair obeys these constraints.
///
/// If we didn't test this, then we might flatten a map `[:k [:l]] to `[:k :l]`, which isn't a valid
/// keyword map in vector form.
pub fn is_valid_keyword_map_k_v<'a>((k, v): (&'a edn::ValueAndSpan, &'a edn::ValueAndSpan)) -> bool {
    if !k.inner.is_keyword() {
        return false;
    }
    match v.inner.as_vector() {
        None => {
            return false;
        },
        Some(ref vs) => {
            if !vs.iter().all(|vv| !vv.inner.is_keyword()) {
                return false;
            }
        },
    }
    return true;
}

pub fn keyword_map_<'a>(input: Stream<'a>) -> ParseResult<Stream<'a>, Stream<'a>> {
    satisfy_map(|v: &'a edn::ValueAndSpan| {
        v.inner.as_map().and_then(|map| {
            if map.iter().all(is_valid_keyword_map_k_v) {
                println!("yes {:?}", map);
                Some(v.keyword_map_stream())
            } else {
                println!("no {:?}", map);
                None
            }
        })
    })
        .parse_lazy(input)
        .into()
}

pub fn keyword_map<'a>() -> Expected<FnParser<Stream<'a>, fn(Stream<'a>) -> ParseResult<Stream<'a>, Stream<'a>>>> {
    parser(keyword_map_ as fn(Stream<'a>) -> ParseResult<Stream<'a>, Stream<'a>>).expected("keyword map")
}

pub fn integer_<'a>(input: Stream<'a>) -> ParseResult<i64, Stream<'a>> {
    satisfy_map(|v: &'a edn::ValueAndSpan| v.inner.as_integer())
        .parse_lazy(input)
        .into()
}

pub fn integer<'a>() -> Expected<FnParser<Stream<'a>, fn(Stream<'a>) -> ParseResult<i64, Stream<'a>>>> {
    parser(integer_ as fn(Stream<'a>) -> ParseResult<i64, Stream<'a>>).expected("integer")
}

pub fn namespaced_keyword_<'a>(input: Stream<'a>) -> ParseResult<&'a edn::Keyword, Stream<'a>> {
    satisfy_map(|v: &'a edn::ValueAndSpan|
        v.inner.as_namespaced_keyword()
         .and_then(|k| if k.is_namespaced() { Some(k) } else { None })
        )
        .parse_lazy(input)
        .into()
}

pub fn namespaced_keyword<'a>() -> Expected<FnParser<Stream<'a>, fn(Stream<'a>) -> ParseResult<&'a edn::Keyword, Stream<'a>>>> {
    parser(namespaced_keyword_ as fn(Stream<'a>) -> ParseResult<&'a edn::Keyword, Stream<'a>>).expected("namespaced_keyword")
}

pub fn forward_keyword_<'a>(input: Stream<'a>) -> ParseResult<&'a edn::Keyword, Stream<'a>> {
    satisfy_map(|v: &'a edn::ValueAndSpan| v.inner.as_namespaced_keyword().and_then(|k| if k.is_forward() && k.is_namespaced() { Some(k) } else { None }))
        .parse_lazy(input)
        .into()
}

pub fn forward_keyword<'a>() -> Expected<FnParser<Stream<'a>, fn(Stream<'a>) -> ParseResult<&'a edn::Keyword, Stream<'a>>>> {
    parser(forward_keyword_ as fn(Stream<'a>) -> ParseResult<&'a edn::Keyword, Stream<'a>>).expected("forward_keyword")
}

pub fn backward_keyword_<'a>(input: Stream<'a>) -> ParseResult<&'a edn::Keyword, Stream<'a>> {
    satisfy_map(|v: &'a edn::ValueAndSpan| v.inner.as_namespaced_keyword().and_then(|k| if k.is_backward() && k.is_namespaced() { Some(k) } else { None }))
        .parse_lazy(input)
        .into()
}

pub fn backward_keyword<'a>() -> Expected<FnParser<Stream<'a>, fn(Stream<'a>) -> ParseResult<&'a edn::Keyword, Stream<'a>>>> {
    parser(backward_keyword_ as fn(Stream<'a>) -> ParseResult<&'a edn::Keyword, Stream<'a>>).expected("backward_keyword")
}

/// Generate a `satisfy` expression that matches a `PlainSymbol` value with the given name.
///
/// We do this rather than using `combine::token` so that we don't need to allocate a new `String`
/// inside a `PlainSymbol` inside a `SpannedValue` inside a `ValueAndSpan` just to match input.
#[macro_export]
macro_rules! def_matches_plain_symbol {
    ( $parser: ident, $name: ident, $input: expr ) => {
        def_parser!($parser, $name, &'a edn::ValueAndSpan, {
            satisfy(|v: &'a edn::ValueAndSpan| {
                match v.inner {
                    edn::SpannedValue::PlainSymbol(ref s) => s.name() == $input,
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
        def_parser!($parser, $name, &'a edn::ValueAndSpan, {
            satisfy(|v: &'a edn::ValueAndSpan| {
                match v.inner {
                    edn::SpannedValue::Keyword(ref s) if !s.is_namespaced() => s.name() == $input,
                    _ => false,
                }
            })
        });
    }
}

/// Generate a `satisfy` expression that matches a `Keyword` value with the given
/// namespace and name.
///
/// We do this rather than using `combine::token` to save allocations.
#[macro_export]
macro_rules! def_matches_namespaced_keyword {
    ( $parser: ident, $name: ident, $input_namespace: expr, $input_name: expr ) => {
        def_parser!($parser, $name, &'a edn::ValueAndSpan, {
            satisfy(|v: &'a edn::ValueAndSpan| {
                match v.inner {
                    edn::SpannedValue::Keyword(ref s) if s.is_namespaced() => {
                        let (ns, n) = s.components();
                        ns == $input_namespace && n == $input_name
                    },
                    _ => false,
                }
            })
        });
    }
}

use combine::primitives::{
    Error,
    Info,
};
use combine::primitives::FastResult::*;

/// Compare to `tuple_parser!` in `combine`.
///
/// This uses edge cases in Rust's hygienic macro system to represent arbitrary values.  That is,
/// `$value: ident` represents both a type in the tuple parameterizing `KeywordMapParser` (since
/// `(A, B, C)` is a valid type declaration) and also a variable value extracted from the underlying
/// instance value.  `$tmp: ident` represents an optional value to return.
///
/// This unrolls the cases.  Each loop iteration reads a token.  It then unrolls the known cases,
/// checking if any case matches the keyword string.  If yes, we parse further.  If no, we move on
/// to the next case.  If no case matches, we fail.
macro_rules! keyword_map_parser {
    ($(($keyword:ident, $value:ident, $tmp:ident)),+) => {
        impl <'a, $($value:),+> Parser for KeywordMapParser<($((&'static str, $value)),+)>
            where $($value: Parser<Input=Stream<'a>>),+
        {
            type Input = Stream<'a>;
            type Output = ($(Option<$value::Output>),+);

            #[allow(non_snake_case)]
            fn parse_lazy(&mut self,
                          mut input: Stream<'a>)
                          -> ConsumedResult<($(Option<$value::Output>),+), Stream<'a>> {
                let ($((ref $keyword, ref mut $value)),+) = (*self).0;
                let mut consumed = false;

                $(
                    let mut $tmp = None;
                )+

                loop {
                    match input.uncons() {
                        Ok(value) => {
                            $(
                                if let Some(ref keyword) = value.inner.as_keyword() {
                                    if &keyword.name() == $keyword {
                                        if $tmp.is_some() {
                                            // Repeated match -- bail out!  Providing good error
                                            // messages is hard; this will do for now.
                                            return ConsumedErr(ParseError::new(input.position(), Error::Unexpected(Info::Token(value))));
                                        }

                                        consumed = true;

                                        $tmp = match $value.parse_lazy(input.clone()) {
                                            ConsumedOk((x, new_input)) => {
                                                input = new_input;
                                                Some(x)
                                            }
                                            EmptyErr(mut err) => {
                                                if let Ok(t) = input.uncons() {
                                                    err.add_error(Error::Unexpected(Info::Token(t)));
                                                }
                                                if consumed {
                                                    return ConsumedErr(err)
                                                } else {
                                                    return EmptyErr(err)
                                                }
                                            }
                                            ConsumedErr(err) => return ConsumedErr(err),
                                            EmptyOk((x, new_input)) => {
                                                input = new_input;
                                                Some(x)
                                            }
                                        };

                                        continue
                                    }
                                }
                            )+

                            // No keyword matched!  Bail out.
                            return ConsumedErr(ParseError::new(input.position(), Error::Unexpected(Info::Token(value))));
                        },
                        Err(err) => {
                            if consumed {
                                return ConsumedOk((($($tmp),+), input))
                            } else {
                                if err == Error::end_of_input() {
                                    return EmptyOk((($($tmp),+), input));
                                }
                                return EmptyErr(ParseError::new(input.position(), err))
                            }
                        },
                    }
                }
            }
        }
    }
}

keyword_map_parser!((Ak, Av, At), (Bk, Bv, Bt));
keyword_map_parser!((Ak, Av, At), (Bk, Bv, Bt), (Ck, Cv, Ct));
keyword_map_parser!((Ak, Av, At), (Bk, Bv, Bt), (Ck, Cv, Ct), (Dk, Dv, Dt));
keyword_map_parser!((Ak, Av, At), (Bk, Bv, Bt), (Ck, Cv, Ct), (Dk, Dv, Dt), (Ek, Ev, Et));
keyword_map_parser!((Ak, Av, At), (Bk, Bv, Bt), (Ck, Cv, Ct), (Dk, Dv, Dt), (Ek, Ev, Et), (Fk, Fv, Ft));
keyword_map_parser!((Ak, Av, At), (Bk, Bv, Bt), (Ck, Cv, Ct), (Dk, Dv, Dt), (Ek, Ev, Et), (Fk, Fv, Ft), (Gk, Gv, Gt));

#[cfg(test)]
mod tests {
    use combine::{
        eof,
        many,
        satisfy,
    };

    use super::*;

    use macros::{
        ResultParser,
    };

    /// A little test parser.
    pub struct Test<'a>(std::marker::PhantomData<&'a ()>);

    def_matches_namespaced_keyword!(Test, add, "db", "add");

    def_parser!(Test, entid, i64, {
        integer()
            .map(|x| x)
            .or(namespaced_keyword().map(|_| -1))
    });

    #[test]
    #[should_panic(expected = r#"keyword map has repeated key: "x""#)]
    fn test_keyword_map_of() {
        keyword_map_of!(("x", Test::entid()),
                        ("x", Test::entid()));
    }

    #[test]
    fn test_iter() {
        // A vector and a map iterated as a keyword map produce the same elements.
        let input = edn::parse::value("[:y 3 4 :x 1 2]").expect("to be able to parse input as EDN");
        assert_eq!(input.child_iter().cloned().map(|x| x.without_spans()).into_iter().collect::<Vec<_>>(),
                   edn::parse::value("[:y 3 4 :x 1 2]").expect("to be able to parse input as EDN").without_spans().into_vector().expect("an EDN vector"));

        let input = edn::parse::value("{:x [1 2] :y [3 4]}").expect("to be able to parse input as EDN");
        assert_eq!(input.keyword_map_iter().cloned().map(|x| x.without_spans()).into_iter().collect::<Vec<_>>(),
                   edn::parse::value("[:y 3 4 :x 1 2]").expect("to be able to parse input as EDN").without_spans().into_vector().expect("an EDN vector"));

        // Parsing a keyword map in map and vector form produces the same elements.  The order (:y
        // before :x) is a foible of our EDN implementation and could be easily changed.
        assert_edn_parses_to!(|| keyword_map().or(vector()).map(|x| x.0.map(|x| x.clone().without_spans()).into_iter().collect::<Vec<_>>()),
                              "{:x [1] :y [2]}",
                              edn::parse::value("[:y 2 :x 1]").expect("to be able to parse input as EDN").without_spans().into_vector().expect("an EDN vector"));

        assert_edn_parses_to!(|| keyword_map().or(vector()).map(|x| x.0.map(|x| x.clone().without_spans()).into_iter().collect::<Vec<_>>()),
                              "[:y 2 :x 1]",
                              edn::parse::value("[:y 2 :x 1]").expect("to be able to parse input as EDN").without_spans().into_vector().expect("an EDN vector"));
    }

    #[test]
    fn test_keyword_map() {
        assert_edn_parses_to!(|| vector().of_exactly(keyword_map_of!(("x", Test::entid()), ("y", Test::entid()))),
                              "[:y 2 :x 1]",
                              (Some(1), Some(2)));

        assert_edn_parses_to!(|| vector().of_exactly(keyword_map_of!(("x", Test::entid()), ("y", Test::entid()))),
                              "[:x 1 :y 2]",
                              (Some(1), Some(2)));

        assert_edn_parses_to!(|| vector().of_exactly(keyword_map_of!(("x", Test::entid()), ("y", Test::entid()))),
                              "[:x 1]",
                              (Some(1), None));

        assert_edn_parses_to!(|| vector().of_exactly(keyword_map_of!(("x", vector().of_exactly(many::<Vec<_>, _>(Test::entid()))),
                                                                     ("y", vector().of_exactly(many::<Vec<_>, _>(Test::entid()))))),
                              "[:x [] :y [1 2]]",
                              (Some(vec![]), Some(vec![1, 2])));

        assert_edn_parses_to!(|| vector().of_exactly(keyword_map_of!(("x", vector().of_exactly(many::<Vec<_>, _>(Test::entid()))),
                                                                     ("y", vector().of_exactly(many::<Vec<_>, _>(Test::entid()))))),
                              "[]",
                              (None, None));
    }

    #[test]
    fn test_keyword_map_failures() {
        assert_parse_failure_contains!(|| vector().of_exactly(keyword_map_of!(("x", Test::entid()), ("y", Test::entid()))),
                              "[:x 1 :x 2]",
                              r#"errors: [Unexpected(Token(ValueAndSpan { inner: Keyword(Keyword(NamespaceableName { namespace: None, name: "x" }))"#);
    }


      // assert_edn_parses_to!(|| keyword_map().or(vector()).map(|x| x.0.map(|x| x.clone().without_spans()).into_iter().collect::<Vec<_>>()), "{:x [1] :y [2]}", vec![]);

        // assert_edn_parses_to!(|| keyword_map().or(vector()).of_exactly((Test::entid(), Test::entid())), "{:x [1] :y [2]}", (-1, 1));

        // assert_edn_parses_to!(|| kw_map().of_exactly((Test::entid(), Test::entid())), "[:a 0 :b 0 1]", (1, 1));

        // assert_edn_parses_to!(|| keyword_map_of(&[(":kw1", Test::entid()),
        //                                           (":kw2", (Test::entid(), Test::entid())),]),
        //                       "{:kw1 0 :kw2 1 :x/y}", ((Some(0), Some((0, 1)))));




        // let input = edn::parse::value("[:x/y]").expect("to be able to parse input as EDN");
        // let par = vector().of_exactly(Test::entid());
        // let stream: Stream = (&input).atom_stream();
        // let result = par.skip(eof()).parse(stream).map(|x| x.0);
        // assert_eq!(result, Ok(1));
    // }

    // #[test]
    // fn test_keyword_map() {
    //     assert_keyword_map_eq!(
    //         "[:foo 1 2 3 :bar 4]",
    //         Some("{:foo [1 2 3] :bar [4]}"));

    //     // Trailing keywords aren't allowed.
    //     assert_keyword_map_eq!(
    //         "[:foo]",
    //         None);
    //     assert_keyword_map_eq!(
    //         "[:foo 2 :bar]",
    //         None);

    //     // Duplicate keywords aren't allowed.
    //     assert_keyword_map_eq!(
    //         "[:foo 2 :foo 1]",
    //         None);

    //     // Starting with anything but a keyword isn't allowed.
    //     assert_keyword_map_eq!(
    //         "[2 :foo 1]",
    //         None);

    //     // Consecutive keywords aren't allowed.
    //     assert_keyword_map_eq!(
    //         "[:foo :bar 1]",
    //         None);

    //     // Empty lists return an empty map.
    //     assert_keyword_map_eq!(
    //         "[]",
    //         Some("{}"));
    // }
}
