// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use itertools::Itertools;
use pretty::{
    BoxAllocator,
    DocAllocator,
    DocBuilder
};

use std::usize;
use std::io;
use std::borrow::Cow;

use types::Value;

pub struct MaxLineLength(pub usize);
pub struct Indent(pub usize);

pub enum Style {
    Clojure(MaxLineLength),
    Expanded(Indent)
}

impl Value {
    /// Return a pretty string representation of this `Value`.
    pub fn to_pretty(&self, style: Style) -> Result<String, io::Error> {
        let mut out = Vec::new();
        self.write_pretty(style, &mut out)?;
        Ok(String::from_utf8_lossy(&out).into_owned())
    }

    /// Write a pretty representation of this `Value` to the given writer.
    pub fn write_pretty<W>(&self, style: Style, out: &mut W) -> Result<(), io::Error> where W: io::Write {
        self.as_doc(&BoxAllocator, &style).1.render(match style {
            Style::Clojure(MaxLineLength(len)) => len,
            Style::Expanded(_) => usize::MAX,
        }, out)
    }

    /// Bracket a collection of values.
    ///
    /// We aim for
    /// [1 2 3]
    /// and fall back if necessary to
    /// [1,
    ///  2,
    ///  3].
    fn bracket<'a, A, T, I>(&'a self, alloc: &'a A, style: &Style, open: T, vs: I, close: T) -> DocBuilder<'a, A>
    where A: DocAllocator<'a>, T: Into<Cow<'a, str>>, I: IntoIterator<Item=&'a Value> {
        let open = open.into();
        let close = close.into();
        let i = vs.into_iter().map(|v| v.as_doc(alloc, style));
        match *style {
            Style::Clojure(_) => {
                let n = open.len();
                let i = i.intersperse(alloc.space());
                alloc.text(open)
                    .append(alloc.concat(i).nest(n))
                    .append(alloc.text(close))
                    .group()
            },
            Style::Expanded(Indent(n)) => {
                let i = i.intersperse(alloc.newline());
                alloc.text(open)
                    .append(alloc.newline()
                        .append(alloc.concat(i))
                        .nest(n))
                    .append(alloc.newline())
                    .append(alloc.text(close))
                    .group()
            }
        }
    }

    fn bracket_map<'a, A, T, I>(&'a self, alloc: &'a A, style: &Style, open: T, pairs: I, close: T) -> DocBuilder<'a, A>
    where A: DocAllocator<'a>, T: Into<Cow<'a, str>>, I: IntoIterator<Item=(&'a Value, &'a Value)>, I::IntoIter: DoubleEndedIterator {
        let open = open.into();
        let close = close.into();
        let i = pairs.into_iter().rev().map(|(k, v)| (k.as_doc(alloc, style), v.as_doc(alloc, style)));
        match *style {
            Style::Clojure(_) => {
                let i = i.map(|(k, v)| k.append(alloc.space()).append(v).group()).intersperse(alloc.space());
                let n = open.len();
                alloc.text(open)
                    .append(alloc.concat(i).nest(n))
                    .append(alloc.text(close))
                    .group()
            },
            Style::Expanded(Indent(n)) => {
                let i = i.map(|(k, v)| k.append(alloc.newline()).append(v).group()).intersperse(alloc.newline());
                alloc.text(open)
                    .append(alloc.newline()
                        .append(alloc.concat(i))
                        .nest(n))
                    .append(alloc.newline())
                    .append(alloc.text(close))
                    .group()
            }
        }
    }

    /// Recursively traverses this value and creates a pretty.rs document.
    /// This pretty printing implementation is optimized for edn queries
    /// readability and limited whitespace expansion.
    pub fn as_doc<'a, A>(&'a self, alloc: &'a A, style: &Style) -> DocBuilder<'a, A>
    where A: DocAllocator<'a> {
        match *self {
            Value::Vector(ref vs) => self.bracket(alloc, style, "[", vs, "]"),
            Value::List(ref vs) => self.bracket(alloc, style, "(", vs, ")"),
            Value::Set(ref vs) => self.bracket(alloc, style, "#{", vs, "}"),
            Value::Map(ref vs) => self.bracket_map(alloc, style, "{", vs, "}"),
            Value::NamespacedSymbol(ref v) => alloc.text(v.namespace.as_ref()).append("/").append(v.name.as_ref()),
            Value::PlainSymbol(ref v) => alloc.text(v.0.as_ref()),
            Value::NamespacedKeyword(ref v) => alloc.text(":").append(v.namespace.as_ref()).append("/").append(v.name.as_ref()),
            Value::Keyword(ref v) => alloc.text(":").append(v.0.as_ref()),
            Value::Text(ref v) => alloc.text("\"").append(v.as_ref()).append("\""),
            _ => alloc.text(self.to_string())
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use parse;

    #[test]
    fn test_pp_io() {
        let string = "$";
        let data = parse::value(string).unwrap().without_spans();

        assert_eq!(data.write_pretty(Style::Clojure(MaxLineLength(40)), &mut Vec::new()).is_ok(), true);
    }

    #[test]
    fn test_pp_types_empty() {
        let string = "[ [ ] ( ) #{ } { }, \"\" ]";
        let data = parse::value(string).unwrap().without_spans();

        assert_eq!(data.to_pretty(Style::Clojure(MaxLineLength(40))).unwrap(), "[[] () #{} {} \"\"]");
    }

    #[test]
    fn test_pp_types_empty_expanded() {
        let string = "[ [ ] ( ) #{ } { }, \"\" ]";
        let data = parse::value(string).unwrap().without_spans();

        // Ending whitespace with `\x20` to avoid editors from removing trailing
        // whitespace and seemingly surprisingly causing this test to fail.
        assert_eq!(data.to_pretty(Style::Expanded(Indent(4))).unwrap(), "\
[
    [
       \x20
    ]
    (
       \x20
    )
    #{
       \x20
    }
    {
       \x20
    }
    \"\"
]");
    }

    #[test]
    fn test_vector() {
        let string = "[1 2 3 4 5 6]";
        let data = parse::value(string).unwrap().without_spans();

        assert_eq!(data.to_pretty(Style::Clojure(MaxLineLength(20))).unwrap(), "[1 2 3 4 5 6]");
        assert_eq!(data.to_pretty(Style::Clojure(MaxLineLength(10))).unwrap(), "\
[1
 2
 3
 4
 5
 6]");
    }

    #[test]
    fn test_vector_expanded() {
        let string = "[1 2 3 4 5 6]";
        let data = parse::value(string).unwrap().without_spans();

        assert_eq!(data.to_pretty(Style::Expanded(Indent(4))).unwrap(), "\
[
    1
    2
    3
    4
    5
    6
]");
    }

    #[test]
    fn test_map() {
        let string = "{:a 1 :b 2 :c 3}";
        let data = parse::value(string).unwrap().without_spans();

        assert_eq!(data.to_pretty(Style::Clojure(MaxLineLength(20))).unwrap(), "{:a 1 :b 2 :c 3}");
        assert_eq!(data.to_pretty(Style::Clojure(MaxLineLength(10))).unwrap(), "\
{:a 1
 :b 2
 :c 3}");
    }

    #[test]
    fn test_map_expanded() {
        let string = "{:a 1 :b 2 :c 3}";
        let data = parse::value(string).unwrap().without_spans();

        assert_eq!(data.to_pretty(Style::Expanded(Indent(4))).unwrap(), "\
{
    :a
    1
    :b
    2
    :c
    3
}");
    }

    #[test]
    fn test_pp_types() {
        let string = "[ 1 2 ( 3.14 ) #{ 4N } { foo/bar 42 :baz/boz 43 } [ ] :five :six/seven eight nine/ten true false nil #f NaN #f -Infinity #f +Infinity ]";
        let data = parse::value(string).unwrap().without_spans();

        assert_eq!(data.to_pretty(Style::Clojure(MaxLineLength(40))).unwrap(), "\
[1
 2
 (3.14)
 #{4N}
 {:baz/boz 43 foo/bar 42}
 []
 :five
 :six/seven
 eight
 nine/ten
 true
 false
 nil
 #f NaN
 #f -Infinity
 #f +Infinity]");
    }

    #[test]
    fn test_pp_types_expanded() {
        let string = "[ 1 2 ( 3.14 ) #{ 4N } { foo/bar 42 :baz/boz 43 } [ ] :five :six/seven eight nine/ten true false nil #f NaN #f -Infinity #f +Infinity ]";
        let data = parse::value(string).unwrap().without_spans();

        // Ending whitespace with `\x20` to avoid editors from removing trailing
        // whitespace and seemingly surprisingly causing this test to fail.
        assert_eq!(data.to_pretty(Style::Expanded(Indent(4))).unwrap(), "\
[
    1
    2
    (
        3.14
    )
    #{
        4N
    }
    {
        :baz/boz
        43
        foo/bar
        42
    }
    [
       \x20
    ]
    :five
    :six/seven
    eight
    nine/ten
    true
    false
    nil
    #f NaN
    #f -Infinity
    #f +Infinity
]");
    }

    #[test]
    fn test_pp_query1() {
        let string = "[:find ?id ?bar ?baz :in $ :where [?id :session/keyword-foo ?symbol1 ?symbol2 \"some string\"] [?tx :db/tx ?ts]]";
        let data = parse::value(string).unwrap().without_spans();

        assert_eq!(data.to_pretty(Style::Clojure(MaxLineLength(40))).unwrap(), "\
[:find
 ?id
 ?bar
 ?baz
 :in
 $
 :where
 [?id
  :session/keyword-foo
  ?symbol1
  ?symbol2
  \"some string\"]
 [?tx :db/tx ?ts]]");
    }

    #[test]
    fn test_pp_query1_expanded() {
        let string = "[:find ?id ?bar ?baz :in $ :where [?id :session/keyword-foo ?symbol1 ?symbol2 \"some string\"] [?tx :db/tx ?ts]]";
        let data = parse::value(string).unwrap().without_spans();

        assert_eq!(data.to_pretty(Style::Expanded(Indent(4))).unwrap(), "\
[
    :find
    ?id
    ?bar
    ?baz
    :in
    $
    :where
    [
        ?id
        :session/keyword-foo
        ?symbol1
        ?symbol2
        \"some string\"
    ]
    [
        ?tx
        :db/tx
        ?ts
    ]
]");
    }

    #[test]
    fn test_pp_query2() {
        let string = "[:find [?id ?bar ?baz] :in [$] :where [?id :session/keyword-foo ?symbol1 ?symbol2 \"some string\"] [?tx :db/tx ?ts] (not-join [?id] [?id :session/keyword-bar _])]";
        let data = parse::value(string).unwrap().without_spans();

        assert_eq!(data.to_pretty(Style::Clojure(MaxLineLength(40))).unwrap(), "\
[:find
 [?id ?bar ?baz]
 :in
 [$]
 :where
 [?id
  :session/keyword-foo
  ?symbol1
  ?symbol2
  \"some string\"]
 [?tx :db/tx ?ts]
 (not-join
  [?id]
  [?id :session/keyword-bar _])]");
    }

    #[test]
    fn test_pp_query2_expanded() {
        let string = "[:find [?id ?bar ?baz] :in [$] :where [?id :session/keyword-foo ?symbol1 ?symbol2 \"some string\"] [?tx :db/tx ?ts] (not-join [?id] [?id :session/keyword-bar _])]";
        let data = parse::value(string).unwrap().without_spans();

        assert_eq!(data.to_pretty(Style::Expanded(Indent(4))).unwrap(), "\
[
    :find
    [
        ?id
        ?bar
        ?baz
    ]
    :in
    [
        $
    ]
    :where
    [
        ?id
        :session/keyword-foo
        ?symbol1
        ?symbol2
        \"some string\"
    ]
    [
        ?tx
        :db/tx
        ?ts
    ]
    (
        not-join
        [
            ?id
        ]
        [
            ?id
            :session/keyword-bar
            _
        ]
    )
]");
    }
}
