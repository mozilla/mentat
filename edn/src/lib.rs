// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate itertools;
extern crate num;
extern crate ordered_float;
extern crate pretty;
extern crate difference;

pub mod symbols;
pub mod types;
pub mod pretty_print;
pub mod utils;
pub mod matcher;

#[macro_use]
pub mod text_diff;

pub mod parse {
    include!(concat!(env!("OUT_DIR"), "/edn.rs"));
}

pub use num::BigInt;
pub use ordered_float::OrderedFloat;
pub use parse::ParseError;
pub use types::{Span, SpannedValue, Value, ValueAndSpan};
pub use symbols::{Keyword, NamespacedKeyword, PlainSymbol, NamespacedSymbol};
