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
    use std::ffi::{
        CString,
        CStr
    };
    use std::os::raw::c_char;

    use mentat::{
        Keyword,
    };

    pub fn c_char_to_string(cchar: *const c_char) -> &'static str {
        let c_str = unsafe { CStr::from_ptr(cchar) };
        c_str.to_str().unwrap_or("")
    }

    pub fn string_to_c_char<T>(r_string: T) -> *mut c_char where T: Into<String> {
        CString::new(r_string.into()).unwrap().into_raw()
    }

    pub fn kw_from_string(keyword_string: &'static str) -> Keyword {
    // TODO: validate. The input might not be a keyword!
        let attr_name = keyword_string.trim_left_matches(":");
        let parts: Vec<&str> = attr_name.split("/").collect();
        Keyword::namespaced(parts[0], parts[1])
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
        unsafe { android::__android_log_write(android::LogLevel::Debug as i32, tag, message) };
    }
}
