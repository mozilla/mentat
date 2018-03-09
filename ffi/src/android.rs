// TODO just use https://github.com/tomaka/android-rs-glue somehow?

use std::os::raw::c_char;
use std::os::raw::c_int;

// Logging
pub const ANDROID_LOG_DEBUG: i32 = 3;
pub const ANDROID_LOG_INFO: i32 = 4;
pub const ANDROID_LOG_WARN: i32 = 5;
pub const ANDROID_LOG_ERROR: i32 = 6;
extern { pub fn __android_log_write(prio: c_int, tag: *const c_char, text: *const c_char) -> c_int; }