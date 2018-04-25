// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use ::std::ffi::{
    CString,
};
use ::std::os::raw::c_char;

use ::std::rc::{
    Rc,
};

use ::std::sync::{
    Arc,
};

use std::fmt;
use ::enum_set::EnumSet;

use ::ordered_float::OrderedFloat;

use ::uuid::Uuid;

use ::chrono::{
    DateTime,
    Timelike,       // For truncation.
};

use ::indexmap::{
    IndexMap,
};

use ::edn::{
    self,
    FromMicros,
    NamespacedKeyword,
    Utc,
};

use values;

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
            Err(r) => Self::new((*r.as_ref()).clone()),
        }
    }
}

impl<T> FromRc<T> for Arc<T> where T: Sized + Clone {
    fn from_rc(val: Rc<T>) -> Self {
        match ::std::rc::Rc::<T>::try_unwrap(val) {
            Ok(v) => Self::new(v),
            Err(r) => Self::new((*r.as_ref()).clone()),
        }
    }

    fn from_arc(val: Arc<T>) -> Self {
        val.clone()
    }
}

// We do this a lot for errors.
pub trait Cloned<T> {
    fn cloned(&self) -> T;
}

impl<T: Clone> Cloned<T> for Rc<T> where T: Sized + Clone {
    fn cloned(&self) -> T {
        (*self.as_ref()).clone()
    }
}

impl<T: Clone> Cloned<T> for Arc<T> where T: Sized + Clone {
    fn cloned(&self) -> T {
        (*self.as_ref()).clone()
    }
}

//
// Use Rc for values.
//
pub type ValueRc<T> = Rc<T>;

/// Represents one entid in the entid space.
///
/// Per https://www.sqlite.org/datatype3.html (see also http://stackoverflow.com/a/8499544), SQLite
/// stores signed integers up to 64 bits in size.  Since u32 is not appropriate for our use case, we
/// use i64 rather than manually truncating u64 to u63 and casting to i64 throughout the codebase.
pub type Entid = i64;

/// An entid that's either already in the store, or newly allocated to a tempid.
/// TODO: we'd like to link this in some way to the lifetime of a particular PartitionMap.
#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct KnownEntid(pub Entid);

impl From<KnownEntid> for Entid {
    fn from(k: KnownEntid) -> Entid {
        k.0
    }
}

impl From<KnownEntid> for TypedValue {
    fn from(k: KnownEntid) -> TypedValue {
        TypedValue::Ref(k.0)
    }
}

/// The attribute of each Mentat assertion has a :db/valueType constraining the value to a
/// particular set.  Mentat recognizes the following :db/valueType values.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
#[repr(u32)]
pub enum ValueType {
    Ref,
    Boolean,
    Instant,
    Long,
    Double,
    String,
    Keyword,
    Uuid,
}

pub type ValueTypeTag = i32;

impl ValueType {
    pub fn all_enums() -> EnumSet<ValueType> {
        // TODO: lazy_static.
        let mut s = EnumSet::new();
        s.insert(ValueType::Ref);
        s.insert(ValueType::Boolean);
        s.insert(ValueType::Instant);
        s.insert(ValueType::Long);
        s.insert(ValueType::Double);
        s.insert(ValueType::String);
        s.insert(ValueType::Keyword);
        s.insert(ValueType::Uuid);
        s
    }
}


impl ::enum_set::CLike for ValueType {
    fn to_u32(&self) -> u32 {
        *self as u32
    }

    unsafe fn from_u32(v: u32) -> ValueType {
        ::std::mem::transmute(v)
    }
}

impl ValueType {
    pub fn into_keyword(self) -> NamespacedKeyword {
        NamespacedKeyword::new("db.type", match self {
            ValueType::Ref => "ref",
            ValueType::Boolean => "boolean",
            ValueType::Instant => "instant",
            ValueType::Long => "long",
            ValueType::Double => "double",
            ValueType::String => "string",
            ValueType::Keyword => "keyword",
            ValueType::Uuid => "uuid",
        })
    }

    pub fn into_typed_value(self) -> TypedValue {
        TypedValue::typed_ns_keyword("db.type", match self {
            ValueType::Ref => "ref",
            ValueType::Boolean => "boolean",
            ValueType::Instant => "instant",
            ValueType::Long => "long",
            ValueType::Double => "double",
            ValueType::String => "string",
            ValueType::Keyword => "keyword",
            ValueType::Uuid => "uuid",
        })
    }

    pub fn into_edn_value(self) -> edn::Value {
        match self {
            ValueType::Ref => values::DB_TYPE_REF.clone(),
            ValueType::Boolean => values::DB_TYPE_BOOLEAN.clone(),
            ValueType::Instant => values::DB_TYPE_INSTANT.clone(),
            ValueType::Long => values::DB_TYPE_LONG.clone(),
            ValueType::Double => values::DB_TYPE_DOUBLE.clone(),
            ValueType::String => values::DB_TYPE_STRING.clone(),
            ValueType::Keyword => values::DB_TYPE_KEYWORD.clone(),
            ValueType::Uuid => values::DB_TYPE_UUID.clone(),
        }
    }
}

impl fmt::Display for ValueType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", match *self {
            ValueType::Ref =>     ":db.type/ref",
            ValueType::Boolean => ":db.type/boolean",
            ValueType::Instant => ":db.type/instant",
            ValueType::Long =>    ":db.type/long",
            ValueType::Double =>  ":db.type/double",
            ValueType::String =>  ":db.type/string",
            ValueType::Keyword => ":db.type/keyword",
            ValueType::Uuid =>    ":db.type/uuid",
        })
    }
}

/// Represents a value that can be stored in a Mentat store.
// TODO: expand to include :db.type/uri. https://github.com/mozilla/mentat/issues/201
// TODO: JSON data type? https://github.com/mozilla/mentat/issues/31
// TODO: BigInt? Bytes?
#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq, Serialize, Deserialize)]
pub enum TypedValue {
    Ref(Entid),
    Boolean(bool),
    Long(i64),
    Double(OrderedFloat<f64>),
    Instant(DateTime<Utc>),               // Use `into()` to ensure truncation.
    // TODO: &str throughout?
    String(ValueRc<String>),
    Keyword(ValueRc<NamespacedKeyword>),
    Uuid(Uuid),                        // It's only 128 bits, so this should be acceptable to clone.
}

/// The values bound in a query specification can be:
///
/// * Vecs of structured values, for multi-valued component attributes or nested expressions.
/// * Single structured values, for single-valued component attributes or nested expressions.
/// * Single typed values, for simple attributes.
///
/// The `Binding` enum defines these three options.
///
/// Datomic also supports structured inputs; at present Mentat does not, but this type
/// would also serve that purpose.
///
/// Note that maps are not ordered, and so `Binding` is neither `Ord` nor `PartialOrd`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Binding {
    Scalar(TypedValue),
    Vec(ValueRc<Vec<Binding>>),
    Map(ValueRc<StructuredMap>),
}

impl<T> From<T> for Binding where T: Into<TypedValue> {
    fn from(value: T) -> Binding {
        Binding::Scalar(value.into())
    }
}

impl Binding {
    pub fn val(self) -> Option<TypedValue> {
        match self {
            Binding::Scalar(v) => Some(v),
            _ => None,
        }
    }
}

/// A pull expression expands a binding into a structure. The returned structure
/// associates attributes named in the input or retrieved from the store with values.
/// This association is a `StructuredMap`.
///
/// Note that 'attributes' in Datomic's case can mean:
/// - Reversed attribute keywords (:artist/_country).
/// - An alias using `:as` (:artist/name :as "Band name").
///
/// We entirely support the former, and partially support the latter -- you can alias
/// using a different keyword only.
#[derive(Debug, Eq, PartialEq)]
pub struct StructuredMap(IndexMap<ValueRc<NamespacedKeyword>, Binding>);

impl Binding {
    /// Returns true if the provided type is `Some` and matches this value's type, or if the
    /// provided type is `None`.
    #[inline]
    pub fn is_congruent_with<T: Into<Option<ValueType>>>(&self, t: T) -> bool {
        t.into().map_or(true, |x| self.matches_type(x))
    }

    #[inline]
    pub fn matches_type(&self, t: ValueType) -> bool {
        self.value_type() == Some(t)
    }

    pub fn value_type(&self) -> Option<ValueType> {
        match self {
            &Binding::Scalar(ref v) => Some(v.value_type()),

            &Binding::Map(_) => None,
            &Binding::Vec(_) => None,
        }
    }
}


impl TypedValue {
    /// Returns true if the provided type is `Some` and matches this value's type, or if the
    /// provided type is `None`.
    #[inline]
    pub fn is_congruent_with<T: Into<Option<ValueType>>>(&self, t: T) -> bool {
        t.into().map_or(true, |x| self.matches_type(x))
    }

    #[inline]
    pub fn matches_type(&self, t: ValueType) -> bool {
        self.value_type() == t
    }

    pub fn value_type(&self) -> ValueType {
        match self {
            &TypedValue::Ref(_) => ValueType::Ref,
            &TypedValue::Boolean(_) => ValueType::Boolean,
            &TypedValue::Long(_) => ValueType::Long,
            &TypedValue::Instant(_) => ValueType::Instant,
            &TypedValue::Double(_) => ValueType::Double,
            &TypedValue::String(_) => ValueType::String,
            &TypedValue::Keyword(_) => ValueType::Keyword,
            &TypedValue::Uuid(_) => ValueType::Uuid,
        }
    }

    /// Construct a new `TypedValue::Keyword` instance by cloning the provided
    /// values and wrapping them in a new `ValueRc`. This is expensive, so this might
    /// be best limited to tests.
    pub fn typed_ns_keyword(ns: &str, name: &str) -> TypedValue {
        NamespacedKeyword::new(ns, name).into()
    }

    /// Construct a new `TypedValue::String` instance by cloning the provided
    /// value and wrapping it in a new `ValueRc`. This is expensive, so this might
    /// be best limited to tests.
    pub fn typed_string(s: &str) -> TypedValue {
        s.into()
    }

    pub fn current_instant() -> TypedValue {
        Utc::now().into()
    }

    /// Construct a new `TypedValue::Instant` instance from the provided
    /// microsecond timestamp.
    pub fn instant(micros: i64) -> TypedValue {
        DateTime::<Utc>::from_micros(micros).into()
    }
}

trait MicrosecondPrecision {
    /// Truncate the provided `DateTime` to microsecond precision.
    fn microsecond_precision(self) -> Self;
}

impl MicrosecondPrecision for DateTime<Utc> {
    fn microsecond_precision(self) -> DateTime<Utc> {
        let nanoseconds = self.nanosecond();
        if nanoseconds % 1000 == 0 {
            return self;
        }
        let microseconds = nanoseconds / 1000;
        let truncated = microseconds * 1000;
        self.with_nanosecond(truncated).expect("valid timestamp")
    }
}

/// Return the current time as a UTC `DateTime` instance with microsecond precision.
pub fn now() -> DateTime<Utc> {
    Utc::now().microsecond_precision()
}

// We don't do From<i64> or From<Entid> 'cos it's ambiguous.

impl From<bool> for TypedValue {
    fn from(value: bool) -> TypedValue {
        TypedValue::Boolean(value)
    }
}

/// Truncate the provided `DateTime` to microsecond precision, and return the corresponding
/// `TypedValue::Instant`.
impl From<DateTime<Utc>> for TypedValue {
    fn from(value: DateTime<Utc>) -> TypedValue {
        TypedValue::Instant(value.microsecond_precision())
    }
}

impl From<Uuid> for TypedValue {
    fn from(value: Uuid) -> TypedValue {
        TypedValue::Uuid(value)
    }
}

impl<'a> From<&'a str> for TypedValue {
    fn from(value: &'a str) -> TypedValue {
        TypedValue::String(ValueRc::new(value.to_string()))
    }
}

impl From<Arc<String>> for TypedValue {
    fn from(value: Arc<String>) -> TypedValue {
        TypedValue::String(ValueRc::from_arc(value))
    }
}

impl From<Rc<String>> for TypedValue {
    fn from(value: Rc<String>) -> TypedValue {
        TypedValue::String(ValueRc::from_rc(value))
    }
}

impl From<String> for TypedValue {
    fn from(value: String) -> TypedValue {
        TypedValue::String(ValueRc::new(value))
    }
}

impl From<Arc<NamespacedKeyword>> for TypedValue {
    fn from(value: Arc<NamespacedKeyword>) -> TypedValue {
        TypedValue::Keyword(ValueRc::from_arc(value))
    }
}

impl From<Rc<NamespacedKeyword>> for TypedValue {
    fn from(value: Rc<NamespacedKeyword>) -> TypedValue {
        TypedValue::Keyword(ValueRc::from_rc(value))
    }
}

impl From<NamespacedKeyword> for TypedValue {
    fn from(value: NamespacedKeyword) -> TypedValue {
        TypedValue::Keyword(ValueRc::new(value))
    }
}

impl From<u32> for TypedValue {
    fn from(value: u32) -> TypedValue {
        TypedValue::Long(value as i64)
    }
}

impl From<i32> for TypedValue {
    fn from(value: i32) -> TypedValue {
        TypedValue::Long(value as i64)
    }
}

impl From<f64> for TypedValue {
    fn from(value: f64) -> TypedValue {
        TypedValue::Double(OrderedFloat(value))
    }
}

impl TypedValue {
    pub fn into_known_entid(self) -> Option<KnownEntid> {
        match self {
            TypedValue::Ref(v) => Some(KnownEntid(v)),
            _ => None,
        }
    }

    pub fn into_entid(self) -> Option<Entid> {
        match self {
            TypedValue::Ref(v) => Some(v),
            _ => None,
        }
    }

    pub fn into_kw(self) -> Option<ValueRc<NamespacedKeyword>> {
        match self {
            TypedValue::Keyword(v) => Some(v),
            _ => None,
        }
    }

    pub fn into_boolean(self) -> Option<bool> {
        match self {
            TypedValue::Boolean(v) => Some(v),
            _ => None,
        }
    }

    pub fn into_long(self) -> Option<i64> {
        match self {
            TypedValue::Long(v) => Some(v),
            _ => None,
        }
    }

    pub fn into_double(self) -> Option<f64> {
        match self {
            TypedValue::Double(v) => Some(v.into_inner()),
            _ => None,
        }
    }

    pub fn into_instant(self) -> Option<DateTime<Utc>> {
        match self {
            TypedValue::Instant(v) => Some(v),
            _ => None,
        }
    }

    pub fn into_timestamp(self) -> Option<i64> {
        match self {
            TypedValue::Instant(v) => Some(v.timestamp()),
            _ => None,
        }
    }

    pub fn into_string(self) -> Option<ValueRc<String>> {
        match self {
            TypedValue::String(v) => Some(v),
            _ => None,
        }
    }

    pub fn into_c_string(self) -> Option<*mut c_char> {
        match self {
            TypedValue::String(v) => {
                // Get an independent copy of the string.
                let s: String = v.cloned();

                // Make a CString out of the new bytes.
                let c: CString = CString::new(s).expect("String conversion failed!");

                // Return a C-owned pointer.
                Some(c.into_raw())
            },
            _ => None,
        }
    }

    pub fn into_kw_c_string(self) -> Option<*mut c_char> {
        match self {
            TypedValue::Keyword(v) => {
                // Get an independent copy of the string.
                let s: String = v.to_string();

                // Make a CString out of the new bytes.
                let c: CString = CString::new(s).expect("String conversion failed!");

                // Return a C-owned pointer.
                Some(c.into_raw())
            },
            _ => None,
        }
    }

    pub fn into_uuid_c_string(self) -> Option<*mut c_char> {
        match self {
            TypedValue::Uuid(v) => {
                // Get an independent copy of the string.
                let s: String = v.hyphenated().to_string();

                // Make a CString out of the new bytes.
                let c: CString = CString::new(s).expect("String conversion failed!");

                // Return a C-owned pointer.
                Some(c.into_raw())
            },
            _ => None,
        }
    }

    pub fn into_uuid(self) -> Option<Uuid> {
        match self {
            TypedValue::Uuid(v) => Some(v),
            _ => None,
        }
    }

    pub fn into_uuid_string(self) -> Option<String> {
        match self {
            TypedValue::Uuid(v) => Some(v.hyphenated().to_string()),
            _ => None,
        }
    }
}

impl Binding {
    pub fn into_known_entid(self) -> Option<KnownEntid> {
        match self {
            Binding::Scalar(TypedValue::Ref(v)) => Some(KnownEntid(v)),
            _ => None,
        }
    }

    pub fn into_entid(self) -> Option<Entid> {
        match self {
            Binding::Scalar(TypedValue::Ref(v)) => Some(v),
            _ => None,
        }
    }

    pub fn into_kw(self) -> Option<ValueRc<NamespacedKeyword>> {
        match self {
            Binding::Scalar(TypedValue::Keyword(v)) => Some(v),
            _ => None,
        }
    }

    pub fn into_boolean(self) -> Option<bool> {
        match self {
            Binding::Scalar(TypedValue::Boolean(v)) => Some(v),
            _ => None,
        }
    }

    pub fn into_long(self) -> Option<i64> {
        match self {
            Binding::Scalar(TypedValue::Long(v)) => Some(v),
            _ => None,
        }
    }

    pub fn into_double(self) -> Option<f64> {
        match self {
            Binding::Scalar(TypedValue::Double(v)) => Some(v.into_inner()),
            _ => None,
        }
    }

    pub fn into_instant(self) -> Option<DateTime<Utc>> {
        match self {
            Binding::Scalar(TypedValue::Instant(v)) => Some(v),
            _ => None,
        }
    }

    pub fn into_timestamp(self) -> Option<i64> {
        match self {
            Binding::Scalar(TypedValue::Instant(v)) => Some(v.timestamp()),
            _ => None,
        }
    }

    pub fn into_string(self) -> Option<ValueRc<String>> {
        match self {
            Binding::Scalar(TypedValue::String(v)) => Some(v),
            _ => None,
        }
    }

    pub fn into_uuid(self) -> Option<Uuid> {
        match self {
            Binding::Scalar(TypedValue::Uuid(v)) => Some(v),
            _ => None,
        }
    }

    pub fn into_uuid_string(self) -> Option<String> {
        match self {
            Binding::Scalar(TypedValue::Uuid(v)) => Some(v.hyphenated().to_string()),
            _ => None,
        }
    }
}

#[test]
fn test_typed_value() {
    assert!(TypedValue::Boolean(false).is_congruent_with(None));
    assert!(TypedValue::Boolean(false).is_congruent_with(ValueType::Boolean));
    assert!(!TypedValue::typed_string("foo").is_congruent_with(ValueType::Boolean));
    assert!(TypedValue::typed_string("foo").is_congruent_with(ValueType::String));
    assert!(TypedValue::typed_string("foo").is_congruent_with(None));
}
