// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use mentat_core::{
    Binding,
};

use errors::{
    ProjectorError,
    Result,
};

/// A `BindingTuple` is any type that can accommodate a Mentat tuple query result of fixed length.
///
/// Currently Rust tuples of length 1 through 6 (i.e., `(A)` through `(A, B, C, D, E, F)`) are
/// supported as are vectors (i.e., `Vec<>`).
pub trait BindingTuple: Sized {
    fn from_binding_vec(expected: usize, vec: Option<Vec<Binding>>) -> Result<Option<Self>>;
}

// This is a no-op, essentially: we can always produce a vector representation of a tuple result.
impl BindingTuple for Vec<Binding> {
    fn from_binding_vec(expected: usize, vec: Option<Vec<Binding>>) -> Result<Option<Self>> {
        match vec {
            None => Ok(None),
            Some(vec) => {
                if expected != vec.len() {
                    Err(ProjectorError::UnexpectedResultsTupleLength(expected, vec.len()))
                } else {
                    Ok(Some(vec))
                }
            },
        }
    }
}

// TODO: generate these repetitive implementations with a little macro.
impl BindingTuple for (Binding,) {
    fn from_binding_vec(expected: usize, vec: Option<Vec<Binding>>) -> Result<Option<Self>> {
        if expected != 1 {
            return Err(ProjectorError::UnexpectedResultsTupleLength(1, expected));
        }
        match vec {
            None => Ok(None),
            Some(vec) => {
                if expected != vec.len() {
                    Err(ProjectorError::UnexpectedResultsTupleLength(expected, vec.len()))
                } else {
                    let mut iter = vec.into_iter();
                    Ok(Some((iter.next().unwrap(),)))
                }
            }
        }
    }
}

impl BindingTuple for (Binding, Binding) {
    fn from_binding_vec(expected: usize, vec: Option<Vec<Binding>>) -> Result<Option<Self>> {
        if expected != 2 {
            return Err(ProjectorError::UnexpectedResultsTupleLength(2, expected));
        }
        match vec {
            None => Ok(None),
            Some(vec) => {
                if expected != vec.len() {
                    Err(ProjectorError::UnexpectedResultsTupleLength(expected, vec.len()))
                } else {
                    let mut iter = vec.into_iter();
                    Ok(Some((iter.next().unwrap(), iter.next().unwrap())))
                }
            }
        }
    }
}

impl BindingTuple for (Binding, Binding, Binding) {
    fn from_binding_vec(expected: usize, vec: Option<Vec<Binding>>) -> Result<Option<Self>> {
        if expected != 3 {
            return Err(ProjectorError::UnexpectedResultsTupleLength(3, expected));
        }
        match vec {
            None => Ok(None),
            Some(vec) => {
                if expected != vec.len() {
                    Err(ProjectorError::UnexpectedResultsTupleLength(expected, vec.len()))
                } else {
                    let mut iter = vec.into_iter();
                    Ok(Some((iter.next().unwrap(), iter.next().unwrap(), iter.next().unwrap())))
                }
            }
        }
    }
}

impl BindingTuple for (Binding, Binding, Binding, Binding) {
    fn from_binding_vec(expected: usize, vec: Option<Vec<Binding>>) -> Result<Option<Self>> {
        if expected != 4 {
            return Err(ProjectorError::UnexpectedResultsTupleLength(4, expected));
        }
        match vec {
            None => Ok(None),
            Some(vec) => {
                if expected != vec.len() {
                    Err(ProjectorError::UnexpectedResultsTupleLength(expected, vec.len()))
                } else {
                    let mut iter = vec.into_iter();
                    Ok(Some((iter.next().unwrap(), iter.next().unwrap(), iter.next().unwrap(), iter.next().unwrap())))
                }
            }
        }
    }
}

impl BindingTuple for (Binding, Binding, Binding, Binding, Binding) {
    fn from_binding_vec(expected: usize, vec: Option<Vec<Binding>>) -> Result<Option<Self>> {
        if expected != 5 {
            return Err(ProjectorError::UnexpectedResultsTupleLength(5, expected));
        }
        match vec {
            None => Ok(None),
            Some(vec) => {
                if expected != vec.len() {
                    Err(ProjectorError::UnexpectedResultsTupleLength(expected, vec.len()))
                } else {
                    let mut iter = vec.into_iter();
                    Ok(Some((iter.next().unwrap(), iter.next().unwrap(), iter.next().unwrap(), iter.next().unwrap(), iter.next().unwrap())))
                }
            }
        }
    }
}

// TODO: allow binding tuples of length more than 6.  Folks who are binding such large tuples are
// probably doing something wrong -- they should investigate a pull expression.
impl BindingTuple for (Binding, Binding, Binding, Binding, Binding, Binding) {
    fn from_binding_vec(expected: usize, vec: Option<Vec<Binding>>) -> Result<Option<Self>> {
        if expected != 6 {
            return Err(ProjectorError::UnexpectedResultsTupleLength(6, expected));
        }
        match vec {
            None => Ok(None),
            Some(vec) => {
                if expected != vec.len() {
                    Err(ProjectorError::UnexpectedResultsTupleLength(expected, vec.len()))
                } else {
                    let mut iter = vec.into_iter();
                    Ok(Some((iter.next().unwrap(), iter.next().unwrap(), iter.next().unwrap(), iter.next().unwrap(), iter.next().unwrap(), iter.next().unwrap())))
                }
            }
        }
    }
}
