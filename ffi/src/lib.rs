// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate mentat;

use std::os::raw::c_char;

pub use mentat::{
    Store,
};

pub mod utils;

pub use utils::strings::{
    c_char_to_string,
};

#[no_mangle]
pub extern "C" fn new_store(uri: *const c_char) -> *mut Store {
    let uri = c_char_to_string(uri);
    let store = Store::open(&uri).expect("expected a store");
    Box::into_raw(Box::new(store))
}
