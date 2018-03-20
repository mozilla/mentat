// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

// TODO just use https://github.com/tomaka/android-rs-glue somehow?

use std::os::raw::c_char;
use std::os::raw::c_int;

// Logging
pub enum LogLevel {
    Debug = 3,
    Info = 4,
    Warn = 5,
    Error = 6,
}

extern { pub fn __android_log_write(prio: c_int, tag: *const c_char, text: *const c_char) -> c_int; }
