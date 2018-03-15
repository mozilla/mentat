// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

pub mod strings {
    use std::os::raw::c_char;
    use std::ffi::{
        CString,
        CStr
    };

    pub fn c_char_to_string(cchar: *const c_char) -> String {
        let c_str = unsafe { CStr::from_ptr(cchar) };
        let r_str = match c_str.to_str() {
            Err(_) => "",
            Ok(string) => string,
        };
        r_str.to_string()
    }

    pub fn string_to_c_char(r_string: String) -> *mut c_char {
        CString::new(r_string).unwrap().into_raw()
    }

    pub fn str_to_c_char(r_string: &str) -> *mut c_char {
        string_to_c_char(r_string.to_string())
    }
}

pub mod log {
    #[cfg(all(target_os="android", not(test)))]
    use std::ffi::CString;

    #[cfg(all(target_os="android", not(test)))]
    use android;

    // TODO far from ideal. And, we might actually want to println in tests.
    #[cfg(all(not(target_os="android"), not(target_os="ios")))]
    pub fn d(_: &str) {}

    #[cfg(all(target_os="ios", not(test)))]
    pub fn d(message: &str) {
        eprintln!("{}", message);
    }

    #[cfg(all(target_os="android", not(test)))]
    pub fn d(message: &str) {
        let message = CString::new(message).unwrap();
        let message = message.as_ptr();
        let tag = CString::new("Mentat").unwrap();
        let tag = tag.as_ptr();
        unsafe { android::__android_log_write(android::ANDROID_LOG_DEBUG, tag, message) };
    }
}
