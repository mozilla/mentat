// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std;
use std::os::raw::{
    c_char,
};

pub use mentat::errors::{
    Result,
};

pub use utils::strings::{
    c_char_to_string,
    string_to_c_char,
};

#[repr(C)]
#[derive(Debug, Clone)]
pub struct ExternTxReport {
    pub txid: i64,
    pub changes: Box<[i64]>,
    pub changes_len: usize
}

#[repr(C)]
#[derive(Debug)]
pub struct ExternTxReportList {
    pub reports: Box<[ExternTxReport]>,
    pub len: usize
}

#[repr(C)]
pub struct ExternResult {
    pub error: *const c_char
}

impl From<Result<()>> for ExternResult {
    fn from(result: Result<()>) -> Self {
        match result {
            Ok(_) => {
                ExternResult {
                    error: std::ptr::null(),
                }
            },
            Err(e) => {
                ExternResult {
                    error: string_to_c_char(e.description().into())
                }
            }
        }
    }
}
