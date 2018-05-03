// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]

///! A pull expression is a function.
///!
///! Its inputs are a store, a schema, and a set of bindings.
///!
///! Its output is a map whose keys are the input bindings and whose values are
///! appropriate structured values to represent the pull expression.
///!
///! For example, the pull expression:
///!
///! ```edn
///! (pull ?person [:person/name
///!                :person/tattoo
///!                {:person/friend [*]}])`
///! ```
///!
///! will return values shaped like:
///!
///! ```edn
///! {:person/name "Alice"                            ; Single-valued attribute
///!                                                  ; Absence: Alice has no tattoos.
///!  :person/friend [                                ; Multi-valued attribute.
///!    {:person/name "Bob"                           ; Nesting and wildcard.
///!     :person/pet ["Harrison", "Hoppy"]}]}
///! ```
///!
///! There will be one such value for each input binding.
///!
///! We fetch layers of a pull expression iteratively: all attributes at the same
///! 'level' can be fetched at the same time and accumulated into maps.
///!
///! Those maps are wrapped in `Rc` for two reasons:
///! - They might occur multiple times when projected from a `:find` query.
///! - They might refer to each other (consider recursion).
///!
///! A nested or recursive pull expression consumes values produced by earlier stages
///! (the recursion with a smaller recursion limit and a growing 'seen' list),
///! generating another layer of mappings.
///!
///! For example, you can imagine the nesting in the earlier pull expression being
///! decomposed into two chained expressions:
///!
///! ```edn
///! (chain
///!   (pull ?person [:person/friend])
///!   (pull ?friend [*]))
///! ```

#[macro_use]
extern crate error_chain;

extern crate rusqlite;

extern crate mentat_core;
extern crate mentat_db;
extern crate mentat_query;
extern crate mentat_query_algebrizer;
extern crate mentat_query_sql;
extern crate mentat_sql;

use std::collections::{
    BTreeMap,
    BTreeSet,
};

use std::iter::{
    once,
};

use mentat_core::{
    Cloned,
    Entid,
    HasSchema,
    NamespacedKeyword,
    Schema,
    StructuredMap,
    ValueRc,
};

use mentat_db::cache;

use mentat_query::{
    PullAttributeSpec,
    PullConcreteAttribute,
};

pub mod errors;

use errors::{
    Result,
};

type PullResults = BTreeMap<Entid, ValueRc<StructuredMap>>;

pub fn pull_attributes_for_entity<A>(schema: &Schema,
                                     db: &rusqlite::Connection,
                                     entity: Entid,
                                     attributes: A) -> Result<StructuredMap>
    where A: IntoIterator<Item=Entid> {
    let attrs = attributes.into_iter()
                          .map(|e| PullAttributeSpec::Attribute(PullConcreteAttribute::Entid(e)))
                          .collect();
    Puller::prepare(schema, attrs)?
        .pull(schema, db, once(entity))
        .map(|m| m.into_iter()
                  .next()
                  .map(|(k, vs)| {
                      assert_eq!(k, entity);
                      vs.cloned()
                  })
                  .unwrap_or_else(StructuredMap::default))
}

pub fn pull_attributes_for_entities<E, A>(schema: &Schema,
                                          db: &rusqlite::Connection,
                                          entities: E,
                                          attributes: A) -> Result<PullResults>
    where E: IntoIterator<Item=Entid>,
          A: IntoIterator<Item=Entid> {
    let attrs = attributes.into_iter()
                          .map(|e| PullAttributeSpec::Attribute(PullConcreteAttribute::Entid(e)))
                          .collect();
    Puller::prepare(schema, attrs)?
        .pull(schema, db, entities)
}

/// A `Puller` constructs on demand a map from a provided set of entity IDs to a set of structured maps.
pub struct Puller {
    // The domain of this map is the set of attributes to fetch.
    // The range is the set of aliases to use in the output.
    attributes: BTreeMap<Entid, ValueRc<NamespacedKeyword>>,
    attribute_spec: cache::AttributeSpec,
}

impl Puller {
    pub fn prepare_simple_attributes(schema: &Schema, attributes: Vec<Entid>) -> Result<Puller> {
        Puller::prepare(schema,
                        attributes.into_iter()
                                  .map(|e| PullAttributeSpec::Attribute(PullConcreteAttribute::Entid(e)))
                                  .collect())
    }

    pub fn prepare(schema: &Schema, attributes: Vec<PullAttributeSpec>) -> Result<Puller> {
        // TODO: eventually this entry point will handle aliasing and that kind of
        // thing. For now it's just a convenience.

        let lookup_name = |i: &Entid| {
            // In the unlikely event that we have an attribute with no name, we invent one.
            ValueRc::new(
                schema.get_ident(*i)
                      .cloned()
                      .unwrap_or(NamespacedKeyword::new("attribute", format!("a{}", i).as_str())))
        };

        let mut names: BTreeMap<Entid, ValueRc<NamespacedKeyword>> = Default::default();
        let mut attrs: BTreeSet<Entid> = Default::default();
        for attr in attributes.iter() {
            match attr {
                PullAttributeSpec::Wildcard => {
                    let attribute_ids = schema.attribute_map.keys();
                    for id in attribute_ids {
                        names.insert(*id, lookup_name(id));
                        attrs.insert(*id);
                    }
                    break;
                },
                PullAttributeSpec::Attribute(PullConcreteAttribute::Ident(ref i)) => {
                    if let Some(entid) = schema.get_entid(i) {
                        names.insert(entid.into(), i.clone());
                        attrs.insert(entid.into());
                    }
                },
                PullAttributeSpec::Attribute(PullConcreteAttribute::Entid(id)) => {
                    names.insert(*id, lookup_name(id));
                    attrs.insert(*id);
                },
            }
        }

        Ok(Puller {
            attributes: names,
            attribute_spec: cache::AttributeSpec::specified(&attrs, schema),
        })
    }

    pub fn pull<E>(&self,
                   schema: &Schema,
                   db: &rusqlite::Connection,
                   entities: E) -> Result<PullResults>
        where E: IntoIterator<Item=Entid> {
        // We implement pull by:
        // - Generating `AttributeCaches` for the provided attributes and entities.
        //   TODO: it would be nice to invert the cache as we build it, rather than have to invert it here.
        // - Recursing. (TODO: we'll need AttributeCaches to not overwrite in case of recursion! And
        //   ideally not do excess work when some entity/attribute pairs are known.)
        // - Building a structure by walking the pull expression with the caches.
        //   TODO: aliases.
        // TODO: limits.
        // TODO: fts.

        // Build a cache for these attributes and entities.
        // TODO: use the store's existing cache!
        let entities: Vec<Entid> = entities.into_iter().collect();
        let cache = cache::AttributeCaches::make_cache_for_entities_and_attributes(
            schema,
            db,
            self.attribute_spec.clone(),
            &entities)?;

        // Now construct the appropriate result format.
        let mut maps = BTreeMap::new();
        for (name, cache) in self.attributes.iter().filter_map(|(a, name)|
            cache.forward_attribute_cache_for_attribute(schema, *a)
                 .map(|cache| (name.clone(), cache))) {

            for e in entities.iter() {
                if let Some(binding) = cache.binding_for_e(*e) {
                    let mut r = maps.entry(*e)
                                    .or_insert(ValueRc::new(StructuredMap::default()));

                    // Get into the inner map so we can accumulate a value.
                    // We can unwrap here because we created all of these maps…
                    let mut m = ValueRc::get_mut(r).unwrap();

                    m.insert(name.clone(), binding);
                }
            }
        }

        Ok(maps)
    }

}
