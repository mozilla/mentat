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
    use std;
    use std::ffi::{
        CString,
        CStr
    };
    use std::os::raw::c_char;
    use std::rc::Rc;

    use mentat::{
        NamespacedKeyword,
    };

    pub fn c_char_to_string(cchar: *const c_char) -> String {
        let c_str = unsafe { CStr::from_ptr(cchar) };
        let r_str = c_str.to_str().unwrap_or("");
        r_str.to_string()
    }

    pub fn string_to_c_char<T>(r_string: T) -> *mut c_char where T: Into<String> {
        CString::new(r_string.into()).unwrap().into_raw()
    }

    pub fn kw_from_string(mut keyword_string: String) -> NamespacedKeyword {
        let attr_name = keyword_string.split_off(1);
        let parts: Vec<&str> = attr_name.split("/").collect();
        NamespacedKeyword::new(parts[0], parts[1])
    }

    pub fn c_char_from_rc(rc_string: Rc<String>) -> *mut c_char {
        if let Some(str_ptr) = unsafe { Rc::into_raw(rc_string).as_ref() } {
            string_to_c_char(str_ptr.clone())
        } else {
            std::ptr::null_mut()
        }
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
