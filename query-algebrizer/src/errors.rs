// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate mentat_query;

use std; // To refer to std::result::Result.

use mentat_core::{
    EdnParseError,
    ValueType,
    ValueTypeSet,
};

use self::mentat_query::{
    PlainSymbol,
};
use std::fmt;
use failure::{
    Backtrace,
    Context,
    Fail,
};

pub type Result<T> = std::result::Result<T, AlgebrizerError>;

#[macro_export]
macro_rules! bail {
    ($e:expr) => (
        return Err($e.into());
    )
}

#[derive(Debug)]
pub struct AlgebrizerError(Box<Context<AlgebrizerErrorKind>>);

impl Fail for AlgebrizerError {
    #[inline]
    fn cause(&self) -> Option<&Fail> {
        self.0.cause()
    }

    #[inline]
    fn backtrace(&self) -> Option<&Backtrace> {
        self.0.backtrace()
    }
}

impl fmt::Display for AlgebrizerError {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&*self.0, f)
    }
}

impl AlgebrizerError {
    #[inline]
    pub fn kind(&self) -> &AlgebrizerErrorKind {
        &*self.0.get_context()
    }
}

impl From<AlgebrizerErrorKind> for AlgebrizerError {
    #[inline]
    fn from(kind: AlgebrizerErrorKind) -> AlgebrizerError {
        AlgebrizerError(Box::new(Context::new(kind)))
    }
}

impl From<Context<AlgebrizerErrorKind>> for AlgebrizerError {
    #[inline]
    fn from(inner: Context<AlgebrizerErrorKind>) -> AlgebrizerError {
        AlgebrizerError(Box::new(inner))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BindingError {
    NoBoundVariable,
    UnexpectedBinding,
    RepeatedBoundVariable, // TODO: include repeated variable(s).

    /// Expected `[[?x ?y]]` but got some other type of binding.  Mentat is deliberately more strict
    /// than Datomic: we won't try to make sense of non-obvious (and potentially erroneous) bindings.
    ExpectedBindRel,

    /// Expected `[[?x ?y]]` or `[?x ...]` but got some other type of binding.  Mentat is
    /// deliberately more strict than Datomic: we won't try to make sense of non-obvious (and
    /// potentially erroneous) bindings.
    ExpectedBindRelOrBindColl,

    /// Expected `[?x1 … ?xN]` or `[[?x1 … ?xN]]` but got some other number of bindings.  Mentat is
    /// deliberately more strict than Datomic: we prefer placeholders to omission.
    InvalidNumberOfBindings { number: usize, expected: usize },
}

#[derive(Clone, Debug, Eq, Fail, PartialEq)]
pub enum AlgebrizerErrorKind {
    #[fail(display = "{} var {} is duplicated", _0, _1)]
    DuplicateVariableError(PlainSymbol, &'static str),

    #[fail(display = "unexpected FnArg")]
    UnsupportedArgument,

    #[fail(display = "value of type {} provided for var {}, expected {}", _0, _1, _2)]
    InputTypeDisagreement(PlainSymbol, ValueType, ValueType),

    #[fail(display = "invalid number of arguments to {}: expected {}, got {}.", _0, _1, _2)]
    InvalidNumberOfArguments(PlainSymbol, usize, usize),

    #[fail(display = "invalid argument to {}: expected {} in position {}.", _0, _1, _2)]
    InvalidArgument(PlainSymbol, &'static str, usize),

    #[fail(display = "invalid argument to {}: expected one of {:?} in position {}.", _0, _1, _2)]
    InvalidArgumentType(PlainSymbol, ValueTypeSet, usize),

    // TODO: flesh this out.
    #[fail(display = "invalid expression in ground constant")]
    InvalidGroundConstant,

    #[fail(display = "invalid limit {} of type {}: expected natural number.", _0, _1)]
    InvalidLimit(String, ValueType),

    #[fail(display = "mismatched bindings in ground")]
    GroundBindingsMismatch,

    #[fail(display = "no entid found for ident: {}", _0)]
    UnrecognizedIdent(String),

    #[fail(display = "no function named {}", _0)]
    UnknownFunction(PlainSymbol),

    #[fail(display = ":limit var {} not present in :in", _0)]
    UnknownLimitVar(PlainSymbol),

    #[fail(display = "unbound variable {} in order clause or function call", _0)]
    UnboundVariable(PlainSymbol),

    // TODO: flesh out.
    #[fail(display = "non-matching variables in 'or' clause")]
    NonMatchingVariablesInOrClause,

    #[fail(display = "non-matching variables in 'not' clause")]
    NonMatchingVariablesInNotClause,

    #[fail(display = "binding error in {}: {:?}", _0, _1)]
    InvalidBinding(PlainSymbol, BindingError),

    #[fail(display = "{}", _0)]
    EdnParseError(#[cause] EdnParseError),
}

impl From<EdnParseError> for AlgebrizerErrorKind {
    fn from(error: EdnParseError) -> AlgebrizerErrorKind {
        AlgebrizerErrorKind::EdnParseError(error)
    }
}

impl From<EdnParseError> for AlgebrizerError {
    fn from(error: EdnParseError) -> AlgebrizerError {
        AlgebrizerErrorKind::from(error).into()
    }
}
