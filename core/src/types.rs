// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use ::std::convert::{
    AsRef,
};

use ::std::ffi::{
    CString,
};

use ::std::ops::{
    Deref,
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
    Cloned,
    FromMicros,
    FromRc,
    Keyword,
    Utc,
    ValueRc,
};

use ::edn::entities::{
    TransactableValueMarker,
};

use values;

use core_traits::{
    Entid,
    KnownEntid,
};

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
    pub fn into_keyword(self) -> Keyword {
        Keyword::namespaced("db.type", match self {
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

    pub fn from_keyword(keyword: &Keyword) -> Option<Self> {
        if keyword.namespace() != Some("db.type") {
            return None;
        }

        return match keyword.name() {
            "ref" => Some(ValueType::Ref),
            "boolean" => Some(ValueType::Boolean),
            "instant" => Some(ValueType::Instant),
            "long" => Some(ValueType::Long),
            "double" => Some(ValueType::Double),
            "string" => Some(ValueType::String),
            "keyword" => Some(ValueType::Keyword),
            "uuid" => Some(ValueType::Uuid),
            _ => None,
        }
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

    pub fn is_numeric(&self) -> bool {
        match self {
            &ValueType::Long | &ValueType::Double => true,
            _ => false
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
    Keyword(ValueRc<Keyword>),
    Uuid(Uuid),                        // It's only 128 bits, so this should be acceptable to clone.
}

/// `TypedValue` is the value type for programmatic use in transaction builders.
impl TransactableValueMarker for TypedValue {}

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
    fn from(value: T) -> Self {
        Binding::Scalar(value.into())
    }
}

impl From<StructuredMap> for Binding {
    fn from(value: StructuredMap) -> Self {
        Binding::Map(ValueRc::new(value))
    }
}

impl From<Vec<Binding>> for Binding {
    fn from(value: Vec<Binding>) -> Self {
        Binding::Vec(ValueRc::new(value))
    }
}

impl Binding {
    pub fn into_scalar(self) -> Option<TypedValue> {
        match self {
            Binding::Scalar(v) => Some(v),
            _ => None,
        }
    }

    pub fn into_vec(self) -> Option<ValueRc<Vec<Binding>>> {
        match self {
            Binding::Vec(v) => Some(v),
            _ => None,
        }
    }

    pub fn into_map(self) -> Option<ValueRc<StructuredMap>> {
        match self {
            Binding::Map(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_scalar(&self) -> Option<&TypedValue> {
        match self {
            &Binding::Scalar(ref v) => Some(v),
            _ => None,
        }
    }

    pub fn as_vec(&self) -> Option<&Vec<Binding>> {
        match self {
            &Binding::Vec(ref v) => Some(v),
            _ => None,
        }
    }

    pub fn as_map(&self) -> Option<&StructuredMap> {
        match self {
            &Binding::Map(ref v) => Some(v),
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
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StructuredMap(pub IndexMap<ValueRc<Keyword>, Binding>);

impl Deref for StructuredMap {
    type Target = IndexMap<ValueRc<Keyword>, Binding>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl StructuredMap {
    pub fn insert<N, B>(&mut self, name: N, value: B) where N: Into<ValueRc<Keyword>>, B: Into<Binding> {
        self.0.insert(name.into(), value.into());
    }
}

impl From<IndexMap<ValueRc<Keyword>, Binding>> for StructuredMap {
    fn from(src: IndexMap<ValueRc<Keyword>, Binding>) -> Self {
        StructuredMap(src)
    }
}

// Mostly for testing.
impl<T> From<Vec<(Keyword, T)>> for StructuredMap where T: Into<Binding> {
    fn from(value: Vec<(Keyword, T)>) -> Self {
        let mut sm = StructuredMap::default();
        for (k, v) in value.into_iter() {
            sm.insert(k, v);
        }
        sm
    }
}

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
    pub fn typed_ns_keyword<S: AsRef<str>, T: AsRef<str>>(ns: S, name: T) -> TypedValue {
        Keyword::namespaced(ns.as_ref(), name.as_ref()).into()
    }

    /// Construct a new `TypedValue::String` instance by cloning the provided
    /// value and wrapping it in a new `ValueRc`. This is expensive, so this might
    /// be best limited to tests.
    pub fn typed_string<S: AsRef<str>>(s: S) -> TypedValue {
        s.as_ref().into()
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

impl From<Box<String>> for TypedValue {
    fn from(value: Box<String>) -> TypedValue {
        TypedValue::String(ValueRc::new(*value))
    }
}

impl From<String> for TypedValue {
    fn from(value: String) -> TypedValue {
        TypedValue::String(ValueRc::new(value))
    }
}

impl From<Arc<Keyword>> for TypedValue {
    fn from(value: Arc<Keyword>) -> TypedValue {
        TypedValue::Keyword(ValueRc::from_arc(value))
    }
}

impl From<Rc<Keyword>> for TypedValue {
    fn from(value: Rc<Keyword>) -> TypedValue {
        TypedValue::Keyword(ValueRc::from_rc(value))
    }
}

impl From<Keyword> for TypedValue {
    fn from(value: Keyword) -> TypedValue {
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

    pub fn into_kw(self) -> Option<ValueRc<Keyword>> {
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

    pub fn into_kw(self) -> Option<ValueRc<Keyword>> {
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

    pub fn into_c_string(self) -> Option<*mut c_char> {
        match self {
            Binding::Scalar(v) => v.into_c_string(),
            _ => None,
        }
    }

    pub fn into_kw_c_string(self) -> Option<*mut c_char> {
        match self {
            Binding::Scalar(v) => v.into_kw_c_string(),
            _ => None,
        }
    }

    pub fn into_uuid_c_string(self) -> Option<*mut c_char> {
        match self {
            Binding::Scalar(v) => v.into_uuid_c_string(),
            _ => None,
        }
    }

    pub fn as_entid(&self) -> Option<&Entid> {
        match self {
            &Binding::Scalar(TypedValue::Ref(ref v)) => Some(v),
            _ => None,
        }
    }

    pub fn as_kw(&self) -> Option<&ValueRc<Keyword>> {
        match self {
            &Binding::Scalar(TypedValue::Keyword(ref v)) => Some(v),
            _ => None,
        }
    }

    pub fn as_boolean(&self) -> Option<&bool> {
        match self {
            &Binding::Scalar(TypedValue::Boolean(ref v)) => Some(v),
            _ => None,
        }
    }

    pub fn as_long(&self) -> Option<&i64> {
        match self {
            &Binding::Scalar(TypedValue::Long(ref v)) => Some(v),
            _ => None,
        }
    }

    pub fn as_double(&self) -> Option<&f64> {
        match self {
            &Binding::Scalar(TypedValue::Double(ref v)) => Some(&v.0),
            _ => None,
        }
    }

    pub fn as_instant(&self) -> Option<&DateTime<Utc>> {
        match self {
            &Binding::Scalar(TypedValue::Instant(ref v)) => Some(v),
            _ => None,
        }
    }

    pub fn as_string(&self) -> Option<&ValueRc<String>> {
        match self {
            &Binding::Scalar(TypedValue::String(ref v)) => Some(v),
            _ => None,
        }
    }

    pub fn as_uuid(&self) -> Option<&Uuid> {
        match self {
            &Binding::Scalar(TypedValue::Uuid(ref v)) => Some(v),
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
