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
        assert!(!cchar.is_null());
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

pub mod error {
    use super::strings::string_to_c_char;
    use std::os::raw::c_char;
    use std::boxed::Box;
    use std::fmt::Display;
    use std::ptr;

    /// Represents an error that occurred on the mentat side. Many mentat FFI functions take a
    /// `*mut ExternError` as the last argument. This is an out parameter that indicates an
    /// error that occurred during that function's execution (if any).
    ///
    /// For functions that use this pattern, if the ExternError's message property is null, then no
    /// error occurred. If the message is non-null then it contains a string description of the
    /// error that occurred.
    ///
    /// Important: This message is allocated on the heap and it is the consumer's responsibility to
    /// free it using `destroy_mentat_string`!
    ///
    /// While this pattern is not ergonomic in Rust, it offers two main benefits:
    ///
    /// 1. It avoids defining a large number of `Result`-shaped types in the FFI consumer, as would
    ///    be required with something like an `struct ExternResult<T> { ok: *mut T, err:... }`
    /// 2. It offers additional type safety over `struct ExternResult { ok: *mut c_void, err:... }`,
    ///    which helps avoid memory safety errors.
    #[repr(C)]
    #[derive(Debug)]
    pub struct ExternError {
        pub message: *mut c_char,
        // TODO: Include an error code here.
    }

    impl Default for ExternError {
        fn default() -> ExternError {
            ExternError { message: ptr::null_mut() }
        }
    }

    /// Translate Result<T, E>, into something C can understand, when T is not `#[repr(C)]`
    ///
    /// - If `result` is `Ok(v)`, moves `v` to the heap and returns a pointer to it, and sets
    ///   `error` to a state indicating that no error occurred (`message` is null).
    /// - If `result` is `Err(e)`, returns a null pointer and stores a string representing the error
    ///   message (which was allocated on the heap and should eventually be freed) into
    ///   `error.message`
    pub unsafe fn translate_result<T, E>(result: Result<T, E>, error: *mut ExternError) -> *mut T
    where E: Display {
        // TODO: can't unwind across FFI...
        assert!(!error.is_null(), "Error output parameter is not optional");
        let error = &mut *error;
        error.message = ptr::null_mut();
        match result {
            Ok(val) => Box::into_raw(Box::new(val)),
            Err(e) => {
                error.message = string_to_c_char(e.to_string());
                ptr::null_mut()
            }
        }
    }

    /// Translate Result<Option<T>, E> into something C can understand, when T is not `#[repr(C)]`.
    ///
    /// - If `result` is `Ok(Some(v))`, moves `v` to the heap and returns a pointer to it, and
    ///   sets `error` to a state indicating that no error occurred (`message` is null).
    /// - If `result` is `Ok(None)` returns a null pointer, but sets `error` to a state indicating
    ///   that no error occurred (`message` is null).
    /// - If `result` is `Err(e)`, returns a null pointer and stores a string representing the error
    ///   message (which was allocated on the heap and should eventually be freed) into
    ///   `error.message`
    pub unsafe fn translate_opt_result<T, E>(result: Result<Option<T>, E>, error: *mut ExternError) -> *mut T
    where E: Display {
        assert!(!error.is_null(), "Error output parameter is not optional");
        let error = &mut *error;
        error.message = ptr::null_mut();
        match result {
            Ok(Some(val)) => Box::into_raw(Box::new(val)),
            Ok(None) => ptr::null_mut(),
            Err(e) => {
                error.message = string_to_c_char(e.to_string());
                ptr::null_mut()
            }
        }
    }

    /// Identical to `translate_result`, but with additional type checking for the case that we have
    /// a `Result<(), E>` (which we're about to drop on the floor).
    pub unsafe fn translate_void_result<E>(result: Result<(), E>, error: *mut ExternError) where E: Display {
        // Note that Box<T> guarantees that if T is zero sized, it's not heap allocated. So not
        // only do we never need to free the return value of this, it would be a problem if someone did.
        translate_result(result, error);
    }
}

