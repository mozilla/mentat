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

use mentat_core::ValueType;

use self::mentat_query::{
    PlainSymbol,
};

error_chain! {
    types {
        Error, ErrorKind, ResultExt, Result;
    }

    errors {
        InputTypeDisagreement(var: PlainSymbol, declared: ValueType, provided: ValueType) {
            description("input type disagreement")
            display("value of type {} provided for var {}, expected {}", provided, var, declared)
        }

        UnknownFunction(name: PlainSymbol) {
            description("no such function")
            display("no function named {}", name)
        }
    
        InvalidNumberOfArguments(name: PlainSymbol, number: usize, expected: usize) {
            description("invalid number of arguments")
            display("invalid number of arguments to {}: expected {}, got {}.", name, expected, number)
        }

        UnboundVariable(name: PlainSymbol) {
            description("unbound variable in order clause or function call")
            display("unbound variable: {}", name)
        }

        NonNumericArgument(function: PlainSymbol, position: usize) {
            description("invalid argument")
            display("invalid argument to {}: expected numeric in position {}.", function, position)
        }

        InvalidLimit(val: String, kind: ValueType) {
            description("invalid limit")
            display("invalid limit {} of type {}: expected natural number.", val, kind)
        }

        NonMatchingVariablesInOrClause {
            // TODO: flesh out.
            description("non-matching variables in 'or' clause")
            display("non-matching variables in 'or' clause")
        }

        NonMatchingVariablesInNotClause {
            // TODO: flesh out.
            description("non-matching variables in 'not' clause")
            display("non-matching variables in 'not' clause")
        }
    }
}

