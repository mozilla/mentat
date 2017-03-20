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

use self::mentat_query::{
    PlainSymbol,
    Variable,
};

error_chain! {
    types {
        Error, ErrorKind, ResultExt, Result;
    }

    errors {
        UnknownFunction(name: PlainSymbol) {
            description("no such function")
            display("no function named {}", name)
        }
    
        InvalidNumberOfArguments(name: PlainSymbol, number: usize, expected: usize) {
            description("invalid number of arguments")
            display("invalid number of arguments to {}: expected {}, got {}.", name, expected, number)
        }

        UnboundVariable(var: Variable) {
            description("unbound variable in function call")
            display("unbound variable: {}", var.0)
        }

        NonNumericArgument(function: PlainSymbol, position: usize) {
            description("invalid argument")
            display("invalid argument to {}: expected numeric in position {}.", function, position)
        }
    }
}

