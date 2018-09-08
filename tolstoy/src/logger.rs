// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

// TODO: use `log` crate.

// TODO it would be nice to be able to pass
// in a logger into Syncer::flow; would allow for a "debug mode"
// and getting useful logs out of clients.
// See https://github.com/mozilla/mentat/issues/571
// Below is some debug Android-friendly logging:

// use std::os::raw::c_char;
// use std::os::raw::c_int;
// use std::ffi::CString;
// pub const ANDROID_LOG_DEBUG: i32 = 3;
// extern { pub fn __android_log_write(prio: c_int, tag: *const c_char, text: *const c_char) -> c_int; }

pub fn d(message: &str) {
    println!("d: {}", message);
    // let message = CString::new(message).unwrap();
    // let message = message.as_ptr();
    // let tag = CString::new("RustyToodle").unwrap();
    // let tag = tag.as_ptr();
    // unsafe { __android_log_write(ANDROID_LOG_DEBUG, tag, message) };
}
