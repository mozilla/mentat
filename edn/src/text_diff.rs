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

use difference::Changeset;

use parse;
use types::Value;
use pretty_print::{Style, Indent};

/// A sum type over all the possible errors encountered while diffing EDN.
#[derive(Debug)]
pub enum EdnDiffError {
    IO(io::Error),
    Parse(parse::ParseError)
}

impl From<io::Error> for EdnDiffError {
    fn from(err: io::Error) -> EdnDiffError {
        EdnDiffError::IO(err)
    }
}
impl From<parse::ParseError> for EdnDiffError {
    fn from(err: parse::ParseError) -> EdnDiffError {
        EdnDiffError::Parse(err)
    }
}

/// Specifies how to split the input strings for the diffing operation,
/// leading to a more or less exact comparison.
pub struct Split<'a>(&'a str);

/// Performs textual diffing between two strings, returning a colorful visual
/// representation of the diff.
pub fn text_diff<'a, T>(orig: &str, edit: &str, split: T) -> String
where T: Into<Option<Split<'a>>> {
    Changeset::new(orig, edit, split.into().unwrap_or_else(|| Split("")).0).to_string()
}

/// Performs textual diffing between two already parsed edn values, returning
/// a colorful visual representation of the diff.
pub fn edn_diff<'a, T>(orig: &Value, edit: &Value, split: T) -> Result<String, EdnDiffError>
where T: Into<Option<Split<'a>>> {
    let a = orig.to_pretty(Style::Expanded(Indent(4)))?;
    let b = edit.to_pretty(Style::Expanded(Indent(4)))?;
    Ok(text_diff(&a, &b, split))
}

/// Performs textual diffing between strings representing edn values, returning
/// a colorful visual representation of the diff.
pub fn raw_edn_diff<'a, T>(orig: &str, edit: &str, split: T) -> Result<String, EdnDiffError>
where T: Into<Option<Split<'a>>> {
    let a = parse::value(orig)?.without_spans();
    let b = parse::value(edit)?.without_spans();
    edn_diff(&a, &b, split)
}

/// A macro utility for asserting equality between two EDN values.
/// Run tests with `-- --nocapture` to get a diff when assertion fails.
#[macro_export]
macro_rules! assert_edn_eq {
    ( $orig:expr, $edit:expr ) => {{
        if $orig != $edit {
            let diff = edn_diff(&$orig, &$edit, None).unwrap();
            println!("{}", diff);
        }
        assert_eq!($orig, $edit);
    }}
}

/// A macro utility for asserting equality between two raw EDN strings.
/// Run tests with `-- --nocapture` to get a diff when assertion fails.
#[macro_export]
macro_rules! assert_raw_edn_eq {
    ( $orig:expr, $edit:expr ) => {{
        let a = parse::value($orig).unwrap().without_spans();
        let b = parse::value($edit).unwrap().without_spans();
        if a != b {
            let diff = edn_diff(&a, &b, None).unwrap();
            println!("{}", diff);
        }
        assert_eq!(a, b);
    }}
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_print_edn_diff_same() {
        let x = "{:a 1 :b 2 :c 3}";
        let y = "{:a 1 :b 2 :c 3}";
        let diff = raw_edn_diff(x, y, None).unwrap();
        assert_eq!(diff, "\
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
    fn test_print_edn_diff_changed_contents() {
        let x = "{:a 1 :b 2 :c 3}";
        let y = "{:a 1 :b 2 :c 4}";
        let diff = raw_edn_diff(x, y, None).unwrap();
        assert_eq!(diff, "\
{
    :a
    1
    :b
    2
    :c
    \u{1b}[91m3\u{1b}[0m\u{1b}[92m4\u{1b}[0m
}");
    }

    #[test]
    fn test_print_edn_diff_changed_whitespace_only() {
        let x = "{:a 1 :b 2 :c 3}";
        let y = "{:a\t1\t:b\t2\t:c\t3}";
        let diff = raw_edn_diff(x, y, None).unwrap();
        assert_eq!(diff, "\
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
    fn test_print_edn_diff_changed_whitespace_and_contents() {
        let x = "{:a 1 :b 2 :c 3}";
        let y = "{:a\t1\t:b\t2\t:c\t4}";
        let diff = raw_edn_diff(x, y, None).unwrap();
        assert_eq!(diff, "\
{
    :a
    1
    :b
    2
    :c
    \u{1b}[91m3\u{1b}[0m\u{1b}[92m4\u{1b}[0m
}");
    }

    #[test]
    fn test_print_edn_diff_line_split() {
        let x = "{:a 1 :b 2 :c 3}";
        let y = "{:a 1 :b 2 :c 4}";
        let diff = raw_edn_diff(x, y, Split("\n")).unwrap();
        assert_eq!(diff, "\
{
    :a
    1
    :b
    2
    :c
\u{1b}[91m    3\u{1b}[0m
\u{1b}[92m    4\u{1b}[0m
}
");
    }

    #[test]
    fn test_print_edn_diff_macro() {
        let x = "{:a 1 :b 2 :c 3}";
        let y = "{:a 1 :b 2 :c 3}";
        assert_edn_eq!(
            parse::value(x).unwrap().without_spans(),
            parse::value(y).unwrap().without_spans());
    }

    #[test]
    #[should_panic]
    fn test_print_edn_diff_macro_panic() {
        let x = "{:a 1 :b 2 :c 3}";
        let y = "{:a 1 :b 2 :c 4}";
        assert_edn_eq!(
            parse::value(x).unwrap().without_spans(),
            parse::value(y).unwrap().without_spans());
    }

    #[test]
    fn test_print_edn_diff_changed_whitespace_macro() {
        let x = "{:a 1 :b 2 :c 3}";
        let y = "{:a\t1\t:b\t2\t:c\t3}";
        assert_edn_eq!(
            parse::value(x).unwrap().without_spans(),
            parse::value(y).unwrap().without_spans());
    }

    #[test]
    #[should_panic]
    fn test_print_edn_diff_changed_whitespace_and_contents_macro_panic() {
        let x = "{:a 1 :b 2 :c 3}";
        let y = "{:a\t1\t:b\t2\t:c\t4}";
        assert_edn_eq!(
            parse::value(x).unwrap().without_spans(),
            parse::value(y).unwrap().without_spans());
    }

    #[test]
    fn test_print_edn_diff_macro_raw() {
        let x = "{:a 1 :b 2 :c 3}";
        let y = "{:a 1 :b 2 :c 3}";
        assert_raw_edn_eq!(x, y);
    }

    #[test]
    #[should_panic]
    fn test_print_edn_diff_macro_raw_panic() {
        let x = "{:a 1 :b 2 :c 3}";
        let y = "{:a 1 :b 2 :c 4}";
        assert_raw_edn_eq!(x, y);
    }

    #[test]
    fn test_print_edn_diff_changed_whitespace_macro_raw() {
        let x = "{:a 1 :b 2 :c 3}";
        let y = "{:a\t1\t:b\t2\t:c\t3}";
        assert_raw_edn_eq!(x, y);
    }

    #[test]
    #[should_panic]
    fn test_print_edn_diff_changed_whitespace_and_contents_macro_raw_panic() {
        let x = "{:a 1 :b 2 :c 3}";
        let y = "{:a\t1\t:b\t2\t:c\t4}";
        assert_raw_edn_eq!(x, y);
    }
}