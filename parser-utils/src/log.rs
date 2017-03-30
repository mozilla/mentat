// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::io::Write;

use combine::{
    ParseError,
    Parser,
    ParseResult,
    Stream,
};

/// println!, but to stderr.
///
/// Doesn't pollute stdout, which is useful when running tests under Emacs, which parses the output
/// of the test suite to format errors and can get confused when user output is interleaved into the
/// stdout stream.
///
/// Cribbed from http://stackoverflow.com/a/27590832.
macro_rules! println_stderr(
    ($($arg:tt)*) => { {
        let r = writeln!(&mut ::std::io::stderr(), $($arg)*);
        r.expect("failed printing to stderr");
    } }
);

#[derive(Clone)]
pub struct Log<P, T>(P, T)
    where P: Parser,
          T: ::std::fmt::Debug;

impl<I, P, T> Parser for Log<P, T>
    where I: Stream,
          I::Item: ::std::fmt::Debug,
          P: Parser<Input = I>,
          P::Output: ::std::fmt::Debug,
          T: ::std::fmt::Debug,
{
    type Input = I;
    type Output = P::Output;

    fn parse_stream(&mut self, input: I) -> ParseResult<Self::Output, I> {
        let head = input.clone().uncons();
        let result = self.0.parse_stream(input.clone());
        match result {
            Ok((ref value, _)) => println_stderr!("{:?}: [{:?} ...] => Ok({:?})", self.1, head.ok(), value),
            Err(_) => println_stderr!("{:?}: [{:?} ...] => Err(_)", self.1, head.ok()),
        }
        result
    }

    fn add_error(&mut self, errors: &mut ParseError<Self::Input>) {
        self.0.add_error(errors);
    }
}

#[inline(always)]
pub fn log<P, T>(p: P, msg: T) -> Log<P, T>
    where P: Parser,
          T: ::std::fmt::Debug,
{
    Log(p, msg)
}

/// We need a trait to define `Parser.log` and have it live outside of the `combine` crate.
pub trait LogParsing: Parser + Sized {
    fn log<T>(self, msg: T) -> Log<Self, T>
        where Self: Sized,
              T: ::std::fmt::Debug;
}

impl<P> LogParsing for P
    where P: Parser,
{
    fn log<T>(self, msg: T) -> Log<Self, T>
        where T: ::std::fmt::Debug,
    {
        log(self, msg)
    }
}
