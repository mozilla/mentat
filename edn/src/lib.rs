// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate ordered_float;
extern crate num;
extern crate pretty;

pub mod symbols;
pub mod types;
pub mod types_pp;
pub mod utils;

pub mod parse {
    include!(concat!(env!("OUT_DIR"), "/edn.rs"));
}

pub use ordered_float::OrderedFloat;
pub use num::BigInt;
pub use types::Value;
pub use symbols::{Keyword, NamespacedKeyword, PlainSymbol, NamespacedSymbol};
