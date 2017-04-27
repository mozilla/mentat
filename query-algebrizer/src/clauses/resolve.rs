// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use mentat_core::{
    TypedValue,
};

use mentat_query::{
    FnArg,
    NonIntegerConstant,
    PlainSymbol,
};

use clauses::ConjoiningClauses;

use errors::{
    Result,
    Error,
    ErrorKind,
};

use types::{
    EmptyBecause,
    QueryValue,
};

/// Argument resolution.
impl ConjoiningClauses {
    /// Take a function argument and turn it into a `QueryValue` suitable for use in a concrete
    /// constraint.
    /// Additionally, do two things:
    /// - Mark the pattern as known-empty if any argument is known non-numeric.
    /// - Mark any variables encountered as numeric.
    pub fn resolve_numeric_argument(&mut self, function: &PlainSymbol, position: usize, arg: FnArg) -> Result<QueryValue> {
        use self::FnArg::*;
        match arg {
            FnArg::Variable(var) => {
                self.constrain_var_to_numeric(var.clone());
                self.column_bindings
                    .get(&var)
                    .and_then(|cols| cols.first().map(|col| QueryValue::Column(col.clone())))
                    .ok_or_else(|| Error::from_kind(ErrorKind::UnboundVariable(var.name())))
            },
            // Can't be an entid.
            EntidOrInteger(i) => Ok(QueryValue::TypedValue(TypedValue::Long(i))),
            Ident(_) |
            SrcVar(_) |
            Constant(NonIntegerConstant::Boolean(_)) |
            Constant(NonIntegerConstant::Text(_)) |
            Constant(NonIntegerConstant::Uuid(_)) |
            Constant(NonIntegerConstant::BigInteger(_)) => {
                self.mark_known_empty(EmptyBecause::NonNumericArgument);
                bail!(ErrorKind::NonNumericArgument(function.clone(), position));
            },
            Constant(NonIntegerConstant::Float(f)) => Ok(QueryValue::TypedValue(TypedValue::Double(f))),
        }
    }


    /// Take a function argument and turn it into a `QueryValue` suitable for use in a concrete
    /// constraint.
    #[allow(dead_code)]
    fn resolve_argument(&self, arg: FnArg) -> Result<QueryValue> {
        use self::FnArg::*;
        match arg {
            FnArg::Variable(var) => {
                self.column_bindings
                    .get(&var)
                    .and_then(|cols| cols.first().map(|col| QueryValue::Column(col.clone())))
                    .ok_or_else(|| Error::from_kind(ErrorKind::UnboundVariable(var.name())))
            },
            EntidOrInteger(i) => Ok(QueryValue::PrimitiveLong(i)),
            Ident(_) => unimplemented!(),     // TODO
            Constant(NonIntegerConstant::Boolean(val)) => Ok(QueryValue::TypedValue(TypedValue::Boolean(val))),
            Constant(NonIntegerConstant::Float(f)) => Ok(QueryValue::TypedValue(TypedValue::Double(f))),
            Constant(NonIntegerConstant::Text(s)) => Ok(QueryValue::TypedValue(TypedValue::typed_string(s.as_str()))),
            Constant(NonIntegerConstant::Uuid(u)) => Ok(QueryValue::TypedValue(TypedValue::Uuid(u))),
            Constant(NonIntegerConstant::BigInteger(_)) => unimplemented!(),
            SrcVar(_) => unimplemented!(),
        }
    }
}
