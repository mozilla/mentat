// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use ::std::rc::{
    Rc,
};

use ::std::sync::{
    Arc,
};

pub trait FromRc<T> {
    fn from_rc(val: Rc<T>) -> Self;
    fn from_arc(val: Arc<T>) -> Self;
}

impl<T> FromRc<T> for Rc<T> where T: Sized + Clone {
    fn from_rc(val: Rc<T>) -> Self {
        val.clone()
    }

    fn from_arc(val: Arc<T>) -> Self {
        match ::std::sync::Arc::<T>::try_unwrap(val) {
            Ok(v) => Self::new(v),
            Err(r) => Self::new(r.cloned()),
        }
    }
}

impl<T> FromRc<T> for Arc<T> where T: Sized + Clone {
    fn from_rc(val: Rc<T>) -> Self {
        match ::std::rc::Rc::<T>::try_unwrap(val) {
            Ok(v) => Self::new(v),
            Err(r) => Self::new(r.cloned()),
        }
    }

    fn from_arc(val: Arc<T>) -> Self {
        val.clone()
    }
}

impl<T> FromRc<T> for Box<T> where T: Sized + Clone {
    fn from_rc(val: Rc<T>) -> Self {
        match ::std::rc::Rc::<T>::try_unwrap(val) {
            Ok(v) => Self::new(v),
            Err(r) => Self::new(r.cloned()),
        }
    }

    fn from_arc(val: Arc<T>) -> Self {
        match ::std::sync::Arc::<T>::try_unwrap(val) {
            Ok(v) => Self::new(v),
            Err(r) => Self::new(r.cloned()),
        }
    }
}

// We do this a lot for errors.
pub trait Cloned<T> {
    fn cloned(&self) -> T;
    fn to_value_rc(&self) -> ValueRc<T>;
}

impl<T: Clone> Cloned<T> for Rc<T> where T: Sized + Clone {
    fn cloned(&self) -> T {
        (*self.as_ref()).clone()
    }

    fn to_value_rc(&self) -> ValueRc<T> {
        ValueRc::from_rc(self.clone())
    }
}

impl<T: Clone> Cloned<T> for Arc<T> where T: Sized + Clone {
    fn cloned(&self) -> T {
        (*self.as_ref()).clone()
    }

    fn to_value_rc(&self) -> ValueRc<T> {
        ValueRc::from_arc(self.clone())
    }
}

impl<T: Clone> Cloned<T> for Box<T> where T: Sized + Clone {
    fn cloned(&self) -> T {
        self.as_ref().clone()
    }

    fn to_value_rc(&self) -> ValueRc<T> {
        ValueRc::new(self.cloned())
    }
}

///
/// This type alias exists to allow us to use different boxing mechanisms for values.
/// This type must implement `FromRc` and `Cloned`, and a `From` implementation must exist for
/// `TypedValue`.
///
pub type ValueRc<T> = Arc<T>;
