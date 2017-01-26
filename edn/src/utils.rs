// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]

use types::Value;

/// Merge the EDN `Value::Map` instance `right` into `left`.  Returns `None` if either `left` or
/// `right` is not a `Value::Map`.
///
/// Keys present in `right` overwrite keys present in `left`.  See also
/// https://clojuredocs.org/clojure.core/merge.
///
/// TODO: implement `merge` for [Value], following the `concat`/`SliceConcatExt` pattern.
pub fn merge(left: &Value, right: &Value) -> Option<Value> {
    match (left, right) {
        (&Value::Map(ref l), &Value::Map(ref r)) => {
            let mut result = l.clone();
            result.extend(r.clone().into_iter());
            Some(Value::Map(result))
        }
        _ => None
    }
}
