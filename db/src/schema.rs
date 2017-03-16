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

use db::TypedSQLValue;
use edn;
use errors::{ErrorKind, Result};
use edn::symbols;
use mentat_core::{
    Attribute,
    Entid,
    EntidMap,
    IdentMap,
    Schema,
    SchemaMap,
    TypedValue,
    Unique,
    ValueType,
};
use metadata;
use metadata::{
    AttributeAlteration,
};

/// Return `Ok(())` if `schema_map` defines a valid Mentat schema.
fn validate_schema_map(entid_map: &EntidMap, schema_map: &SchemaMap) -> Result<()> {
    for (entid, attribute) in schema_map {
        let ident = || entid_map.get(entid).map(|ident| ident.to_string()).unwrap_or(entid.to_string());
        if attribute.unique == Some(Unique::Value) && !attribute.index {
            bail!(ErrorKind::BadSchemaAssertion(format!(":db/unique :db/unique_value without :db/index true for entid: {}", ident())))
        }
        if attribute.unique == Some(Unique::Identity) && !attribute.index {
            bail!(ErrorKind::BadSchemaAssertion(format!(":db/unique :db/unique_identity without :db/index true for entid: {}", ident())))
        }
        if attribute.fulltext && attribute.value_type != ValueType::String {
            bail!(ErrorKind::BadSchemaAssertion(format!(":db/fulltext true without :db/valueType :db.type/string for entid: {}", ident())))
        }
        if attribute.fulltext && !attribute.index {
            bail!(ErrorKind::BadSchemaAssertion(format!(":db/fulltext true without :db/index true for entid: {}", ident())))
        }
        if attribute.component && attribute.value_type != ValueType::Ref {
            bail!(ErrorKind::BadSchemaAssertion(format!(":db/isComponent true without :db/valueType :db.type/ref for entid: {}", ident())))
        }
        // TODO: consider warning if we have :db/index true for :db/valueType :db.type/string,
        // since this may be inefficient.  More generally, we should try to drive complex
        // :db/valueType (string, uri, json in the future) users to opt-in to some hash-indexing
        // scheme, as discussed in https://github.com/mozilla/mentat/issues/69.
    }
    Ok(())
}

#[derive(Clone,Debug,Default,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub struct AttributeBuilder {
    value_type: Option<ValueType>,
    multival: Option<bool>,
    unique: Option<Option<Unique>>,
    index: Option<bool>,
    fulltext: Option<bool>,
    component: Option<bool>,
}

impl AttributeBuilder {
    pub fn value_type<'a>(&'a mut self, value_type: ValueType) -> &'a mut Self {
        self.value_type = Some(value_type);
        self
    }

    pub fn multival<'a>(&'a mut self, multival: bool) -> &'a mut Self {
        self.multival = Some(multival);
        self
    }

    pub fn unique<'a>(&'a mut self, unique: Option<Unique>) -> &'a mut Self {
        self.unique = Some(unique);
        self
    }

    pub fn index<'a>(&'a mut self, index: bool) -> &'a mut Self {
        self.index = Some(index);
        self
    }

    pub fn fulltext<'a>(&'a mut self, fulltext: bool) -> &'a mut Self {
        self.fulltext = Some(fulltext);
        self
    }

    pub fn component<'a>(&'a mut self, component: bool) -> &'a mut Self {
        self.component = Some(component);
        self
    }

    pub fn is_valid_install_attribute(&self) -> bool {
        self.value_type.is_some()
    }

    pub fn is_valid_alter_attribute(&self) -> bool {
        self.value_type.is_none() && self.fulltext.is_none()
    }

    pub fn build(&self) -> Attribute {
        let mut attribute = Attribute::default();
        if let Some(value_type) = self.value_type {
            attribute.value_type = value_type;
        }
        if let Some(fulltext) = self.fulltext {
            attribute.fulltext = fulltext;
        }
        if let Some(multival) = self.multival {
            attribute.multival = multival;
        }
        if let Some(ref unique) = self.unique {
            attribute.unique = unique.clone();
        }
        if let Some(index) = self.index {
            attribute.index = index;
        }
        if let Some(component) = self.component {
            attribute.component = component;
        }

        attribute
    }

    pub fn mutate(&self, attribute: &mut Attribute) -> Vec<AttributeAlteration> {
        let mut mutations = Vec::new();
        if let Some(multival) = self.multival {
            if multival != attribute.multival {
                attribute.multival = multival;
                mutations.push(AttributeAlteration::Cardinality);
            }
        }
        if let Some(ref unique) = self.unique {
            if *unique != attribute.unique {
                attribute.unique = unique.clone();
                mutations.push(AttributeAlteration::Unique);
            }
        }
        if let Some(index) = self.index {
            if index != attribute.index {
                attribute.index = index;
                mutations.push(AttributeAlteration::Index);
            }
        }
        if let Some(component) = self.component {
            if component != attribute.component {
                attribute.component = component;
                mutations.push(AttributeAlteration::IsComponent);
            }
        }

        mutations
    }
}

pub trait SchemaBuilding {
    fn require_ident(&self, entid: Entid) -> Result<&symbols::NamespacedKeyword>;
    fn require_entid(&self, ident: &symbols::NamespacedKeyword) -> Result<Entid>;
    fn require_attribute_for_entid(&self, entid: Entid) -> Result<&Attribute>;
    fn from_ident_map_and_schema_map(ident_map: IdentMap, schema_map: SchemaMap) -> Result<Schema>;
    fn from_ident_map_and_triples<U>(ident_map: IdentMap, assertions: U) -> Result<Schema>
        where U: IntoIterator<Item=(symbols::NamespacedKeyword, symbols::NamespacedKeyword, TypedValue)>;
}

impl SchemaBuilding for Schema {
    fn require_ident(&self, entid: Entid) -> Result<&symbols::NamespacedKeyword> {
        self.get_ident(entid).ok_or(ErrorKind::UnrecognizedEntid(entid).into())
    }

    fn require_entid(&self, ident: &symbols::NamespacedKeyword) -> Result<Entid> {
        self.get_entid(&ident).ok_or(ErrorKind::UnrecognizedIdent(ident.to_string()).into())
    }

    fn require_attribute_for_entid(&self, entid: Entid) -> Result<&Attribute> {
        self.attribute_for_entid(entid).ok_or(ErrorKind::UnrecognizedEntid(entid).into())
    }

    /// Create a valid `Schema` from the constituent maps.
    fn from_ident_map_and_schema_map(ident_map: IdentMap, schema_map: SchemaMap) -> Result<Schema> {
        let entid_map: EntidMap = ident_map.iter().map(|(k, v)| (v.clone(), k.clone())).collect();

        validate_schema_map(&entid_map, &schema_map)?;

        Ok(Schema {
            ident_map: ident_map,
            entid_map: entid_map,
            schema_map: schema_map,
        })
    }

    /// Turn vec![(NamespacedKeyword(:ident), NamespacedKeyword(:key), TypedValue(:value)), ...] into a Mentat `Schema`.
    fn from_ident_map_and_triples<U>(ident_map: IdentMap, assertions: U) -> Result<Schema>
        where U: IntoIterator<Item=(symbols::NamespacedKeyword, symbols::NamespacedKeyword, TypedValue)>{

        let entid_assertions: Result<Vec<(Entid, Entid, TypedValue)>> = assertions.into_iter().map(|(symbolic_ident, symbolic_attr, value)| {
            let ident: i64 = *ident_map.get(&symbolic_ident).ok_or(ErrorKind::UnrecognizedIdent(symbolic_ident.to_string()))?;
            let attr: i64 = *ident_map.get(&symbolic_attr).ok_or(ErrorKind::UnrecognizedIdent(symbolic_attr.to_string()))?;
            Ok((ident, attr, value))
        }).collect();
        let mut schema = Schema::from_ident_map_and_schema_map(ident_map, SchemaMap::default())?;
        metadata::update_schema_map_from_entid_triples(&mut schema.schema_map, entid_assertions?)?;
        Ok(schema)
    }
}

pub trait SchemaTypeChecking {
    /// Do schema-aware typechecking and coercion.
    ///
    /// Either assert that the given value is in the attribute's value set, or (in limited cases)
    /// coerce the given value into the attribute's value set.
    fn to_typed_value(&self, value: &edn::Value, attribute: &Attribute) -> Result<TypedValue>;
}

impl SchemaTypeChecking for Schema {
    fn to_typed_value(&self, value: &edn::Value, attribute: &Attribute) -> Result<TypedValue> {
        // TODO: encapsulate entid-ident-attribute for better error messages.
        match TypedValue::from_edn_value(value) {
            // We don't recognize this EDN at all.  Get out!
            None => bail!(ErrorKind::BadEDNValuePair(value.clone(), attribute.value_type.clone())),
            Some(typed_value) => match (&attribute.value_type, typed_value) {
                // Most types don't coerce at all.
                (&ValueType::Boolean, tv @ TypedValue::Boolean(_)) => Ok(tv),
                (&ValueType::Long, tv @ TypedValue::Long(_)) => Ok(tv),
                (&ValueType::Double, tv @ TypedValue::Double(_)) => Ok(tv),
                (&ValueType::String, tv @ TypedValue::String(_)) => Ok(tv),
                (&ValueType::Keyword, tv @ TypedValue::Keyword(_)) => Ok(tv),
                // Ref coerces a little: we interpret some things depending on the schema as a Ref.
                (&ValueType::Ref, TypedValue::Long(x)) => Ok(TypedValue::Ref(x)),
                (&ValueType::Ref, TypedValue::Keyword(ref x)) => self.require_entid(&x).map(|entid| TypedValue::Ref(entid)),
                // Otherwise, we have a type mismatch.
                (value_type, _) => bail!(ErrorKind::BadEDNValuePair(value.clone(), value_type.clone())),
            }
        }
    }
}
