// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::io;
use pretty;

use types::Value;
use symbols;

/// Helper for glueing a sequence of pretty.rs documents together. All of these
/// given `$node`s can be anything that converts into a pretty.rs document.
macro_rules! print_seq {
    ( $pp:ident: $( $node:expr ),+ ) => {{
        let mut doc = $pp.nil();
        $( doc = doc.append($node); )*
        doc
    }}
}

/// Helper for joining a sequence of nodes from an `$iterator` into a pretty.rs
/// document, separated by zero or more `$separator` nodes. All of these
/// given `$node`s can be anything that converts into a pretty.rs document.
/// The second variant of this macro creates a pretty.rs document which nests
/// all the nodes from an `$iterator` at a specified `$indent` depth, surrounded
/// by newlines. If `$indent` is zero, a non-indented pretty.rs document is returned.
macro_rules! print_iter {
    ( $pp:ident: $iterator:expr, $( $separator:expr ),* ) => {{
        let mut doc = $pp.nil();
        for (index, value) in $iterator.enumerate() {
            if index != 0 { $( doc = doc.append($separator); )* }
            doc = doc.append(value)
        }
        doc
    }};
    ( $pp:ident: $iterator:expr, $( $separator:expr ),*; $indent:expr ) => {{
        let children = print_iter!($pp: $iterator, $( $separator ),*);
        match $indent {
            0 => children,
            _ => print_seq!($pp: print_seq!($pp: $pp.newline(), children).nest($indent), $pp.newline()),
        }
    }}
}

impl Value {
    /// Recursively traverses this value and pretty prints it to a string.
    pub fn pretty_print_to_string(&self) -> Result<String, io::Error> {
        self.pretty_print_to_writable(80, Vec::new())
            .map(|writable| String::from_utf8_lossy(&writable).into_owned())
    }

    /// Recursively traverses this value and pretty prints it to memory or any
    /// other given stream.
    pub fn pretty_print_to_writable<W>(&self, width: usize, mut out: W) -> Result<W, io::Error> where W: io::Write {
        self.as_doc(&pretty::BoxAllocator).1.render(width, &mut out).map(|()| out)
    }

    /// Recursively traverses this value and creates a pretty.rs document.
    /// This pretty printing implementation is optimized for edn queries
    /// readability and limited whitespace expansion.
    pub fn as_doc<'a, A>(&'a self, pp: &'a A) -> pretty::DocBuilder<'a, A>
    where A: pretty::DocAllocator<'a> {
        // Returns the desired indentation amount for a given collection of Values.
        macro_rules! indent { ( $v:expr ) => {
            if $v.len() < 2 || $v.iter().all(|x| x.is_symbol()) { 0 } else { 4 }
        }}
        // Maps a collection of Values to pretty.rs documents.
        macro_rules! map_values { ( $v:expr ) => {
            $v.iter().map(|x| x.as_doc(pp))
        }}
        // Maps a collection of (Value, Value) pairs to pretty.rs documents.
        macro_rules! map_pairs { ( $v:expr ) => {
            $v.iter().map(|(x, y)| print_seq!(pp: x.as_doc(pp), " ", y.as_doc(pp)));
        }}
        match *self {
            Value::Vector(ref v) => {
                print_seq!(pp: "[", print_iter!(pp: map_values!(v), " "; indent!(v)), "]")
            }
            Value::List(ref v) => {
                print_seq!(pp: "(", print_iter!(pp: map_values!(v), " "; indent!(v)), ")")
            }
            Value::Set(ref v) => {
                print_seq!(pp: "#{", print_iter!(pp: map_values!(v), " "), "}")
            }
            Value::Map(ref v) => {
                print_seq!(pp: "{", print_iter!(pp: map_pairs!(v), " "), "}")
            }
            Value::Text(ref v) => {
                print_seq!(pp: "\"", v.as_ref(), "\"")
            }
            Value::NamespacedSymbol(ref v) => {
                print_seq!(pp: v.namespace.as_ref(), "/", v.name.as_ref())
            }
            Value::NamespacedKeyword(ref v) => {
                print_seq!(pp: ":", v.namespace.as_ref(), "/", v.name.as_ref())
            }
            Value::PlainSymbol(symbols::PlainSymbol(ref name)) => {
                print_seq!(pp: name.as_ref())
            }
            Value::Keyword(symbols::Keyword(ref name)) => {
                // Special treatment for ":in" and ":where" keywords to always
                // place them on a newline in the outputted pretty.rs document.
                // TODO: Add more "special" keywords here, or remove this?
                let name = name.as_ref();
                match name {
                    "in" | "where" => print_seq!(pp: pp.newline(), ":", name),
                    _ => print_seq!(pp: ":", name)
                }
            }
            _ => pp.text(self.to_string())
        }
    }
}

#[cfg(test)]
mod test {
    use parse;

    // To output an edn value to stdout, use the following snippet:
    // value.pretty_print_to_writable(80, ::std::io::stdout()).ok();

    #[test]
    fn test_pp_io() {
        let string = "$";
        let data = parse::value(string).unwrap();

        assert_eq!(data.pretty_print_to_writable(80, Vec::new()).is_ok(), true);
    }

    #[test]
    fn test_pp_types_empty() {
        let string = "[ [ ] ( ) #{ } { }, \"\" ]";
        let data = parse::value(string).unwrap();

        let pretty = data.pretty_print_to_string();
        assert_eq!(pretty.unwrap(), "\
[
    [] () #{} {} \"\"
]");
    }

    #[test]
    fn test_pp_types() {
        let string = "[ 1 2 ( 3.14 ) #{ 4N } { foo/bar 42 :baz/boz 43 } [ ] :five :six/seven eight nine/ten true false nil #f NaN #f -Infinity #f +Infinity ]";
        let data = parse::value(string).unwrap();

        let pretty = data.pretty_print_to_string();
        assert_eq!(pretty.unwrap(),"\
[
    1 2 (3.14) #{4N} {foo/bar 42 :baz/boz 43} [] :five :six/seven eight nine/ten true false nil #f NaN #f -Infinity #f +Infinity
]");
    }

    #[test]
    fn test_pp_query1() {
        let string = "[:find ?id ?bar ?baz :in $ :where [?id :session/keyword-foo ?symbol1 ?symbol2 \"some string\"] [?tx :db/tx ?ts]]";
        let data = parse::value(string).unwrap();

        let pretty = data.pretty_print_to_string();
        assert_eq!(pretty.unwrap(), "\
[
    :find ?id ?bar ?baz\x20
    :in $\x20
    :where [
        ?id :session/keyword-foo ?symbol1 ?symbol2 \"some string\"
    ] [
        ?tx :db/tx ?ts
    ]
]");
    }

    #[test]
    fn test_pp_query2() {
        let string = "[:find [?id ?bar ?baz] :in [$] :where [?id :session/keyword-foo ?symbol1 ?symbol2 \"some string\"] [?tx :db/tx ?ts] (not-join [?id] [?id :session/keyword-bar _])]";
        let data = parse::value(string).unwrap();

        let pretty = data.pretty_print_to_string();
        assert_eq!(pretty.unwrap(), "\
[
    :find [?id ?bar ?baz]\x20
    :in [$]\x20
    :where [
        ?id :session/keyword-foo ?symbol1 ?symbol2 \"some string\"
    ] [
        ?tx :db/tx ?ts
    ] (
        not-join [?id] [
            ?id :session/keyword-bar _
        ]
    )
]");
    }
}
