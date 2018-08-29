// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![macro_use]

// We have a little bit of a dilemma in Mentat.
// The public data format for transacting is, fundamentally, a big string: EDN.
// The internal data format for transacting is required to encode the complexities of
// processing that format: temporary IDs, lookup refs, input spans, etc.
//
// See edn::entities::Entity and all of its child enums to see how complex this gets.
//
// A programmatic consumer doesn't want to build something that looks like:
//
//     Entity::AddOrRetract {
//         op: OpType::Add,
//         e: EntidOrLookupRefOrTempId::LookupRef(LookupRef {
//             a: Entid::Ident(Keyword::namespaced("test", "a1")),
//             v: Value::Text("v1".into()),
//         }),
//         a: Entid::Ident(kw!(:test/a)),
//         v: AtomOrLookupRefOrVectorOrMapNotation::Atom(ValueAndSpan::new(SpannedValue::Text("v".into()), Span(44, 47))),
//     }));
//
// but neither do they want to pay the cost of parsing
//
//    [[:test/a1 "v1"] :test/a "v"]
//
// at runtime.
//
// It's tempting to think that we can do something 'easy' here -- to skip the hard work of transacting
// tempids, for example -- but to do so will hobble the system for little payoff. It's also worth
// remembering that the transactor does significant validation work, which we don't want to
// reimplement here.
//
// The win we seek is to make it easier to _write_ these inputs without significantly restricting
// what can be said.
//
// There are two ways we could go from here.
//
// The first is to expose tx parsing as a macro: parse that string at compile time into the
// equivalent `Entity` data structure. That's fine for completely static input data.
//
// The second is to expose a declarative, programmatic builder pattern for constructing entities.
//
// We probably need both, but this file provides the latter.

use edn::{
    InternSet,
    PlainSymbol,
    ValueRc,
};
use edn::entities::{
    AttributePlace,
    Entity,
    EntityPlace,
    LookupRef,
    OpType,
    TempId,
    TxFunction,
    ValuePlace,
};

use core_traits::{
    TypedValue,
};

use mentat_core::{
    TxReport,
};

use ::InProgress;

use public_traits::errors::{
    Result,
};

pub type Terms = (Vec<Entity<TypedValue>>, InternSet<TempId>);

pub struct TermBuilder {
    tempids: InternSet<TempId>,
    terms: Vec<Entity<TypedValue>>,
}

pub struct EntityBuilder<T: BuildTerms + Sized> {
    builder: T,
    entity: EntityPlace<TypedValue>,
}

pub trait BuildTerms where Self: Sized {
    fn named_tempid<I>(&mut self, name: I) -> ValueRc<TempId> where I: Into<String>;
    fn describe_tempid(self, name: &str) -> EntityBuilder<Self>;
    fn describe<E>(self, entity: E) -> EntityBuilder<Self> where E: Into<EntityPlace<TypedValue>>;
    fn add<E, A, V>(&mut self, e: E, a: A, v: V) -> Result<()>
    where E: Into<EntityPlace<TypedValue>>,
          A: Into<AttributePlace>,
          V: Into<ValuePlace<TypedValue>>;
    fn retract<E, A, V>(&mut self, e: E, a: A, v: V) -> Result<()>
    where E: Into<EntityPlace<TypedValue>>,
          A: Into<AttributePlace>,
          V: Into<ValuePlace<TypedValue>>;
}

impl BuildTerms for TermBuilder {
    fn named_tempid<I>(&mut self, name: I) -> ValueRc<TempId> where I: Into<String> {
        self.tempids.intern(TempId::External(name.into()))
    }

    fn describe_tempid(mut self, name: &str) -> EntityBuilder<Self> {
        let e = self.named_tempid(name);
        self.describe(e)
    }

    fn describe<E>(self, entity: E) -> EntityBuilder<Self> where E: Into<EntityPlace<TypedValue>> {
        EntityBuilder {
            builder: self,
            entity: entity.into(),
        }
    }

    fn add<E, A, V>(&mut self, e: E, a: A, v: V) -> Result<()>
    where E: Into<EntityPlace<TypedValue>>,
          A: Into<AttributePlace>,
          V: Into<ValuePlace<TypedValue>> {
        self.terms.push(Entity::AddOrRetract { op: OpType::Add, e: e.into(), a: a.into(), v: v.into() });
        Ok(())
    }

    fn retract<E, A, V>(&mut self, e: E, a: A, v: V) -> Result<()>
    where E: Into<EntityPlace<TypedValue>>,
          A: Into<AttributePlace>,
          V: Into<ValuePlace<TypedValue>> {
        self.terms.push(Entity::AddOrRetract { op: OpType::Retract, e: e.into(), a: a.into(), v: v.into() });
        Ok(())
    }
}

impl TermBuilder {
    pub fn build(self) -> Result<Terms> {
        Ok((self.terms, self.tempids))
    }

    pub fn new() -> TermBuilder {
        TermBuilder {
            tempids: InternSet::new(),
            terms: vec![],
        }
    }

    pub fn is_empty(&self) -> bool {
        self.terms.is_empty()
    }

    #[allow(dead_code)]
    pub fn numbered_tempid(&mut self, id: i64) -> ValueRc<TempId> {
        self.tempids.intern(TempId::Internal(id))
    }

    pub fn lookup_ref<A, V>(a: A, v: V) -> LookupRef<TypedValue>
    where A: Into<AttributePlace>,
          V: Into<TypedValue> {
        LookupRef { a: a.into(), v: v.into() }
    }

    pub fn tx_function(op: &str) -> TxFunction {
        TxFunction { op: PlainSymbol::plain(op) }
    }
}

impl<T> EntityBuilder<T> where T: BuildTerms {
    pub fn finish(self) -> (T, EntityPlace<TypedValue>) {
        (self.builder, self.entity)
    }

    pub fn add<A, V>(&mut self, a: A, v: V) -> Result<()>
    where A: Into<AttributePlace>,
          V: Into<ValuePlace<TypedValue>> {
        self.builder.add(self.entity.clone(), a, v)
    }

    pub fn retract<A, V>(&mut self, a: A, v: V) -> Result<()>
    where A: Into<AttributePlace>,
          V: Into<ValuePlace<TypedValue>> {
        self.builder.retract(self.entity.clone(), a, v)
    }
}

pub struct InProgressBuilder<'a, 'c> {
    in_progress: InProgress<'a, 'c>,
    builder: TermBuilder,
}

impl<'a, 'c> InProgressBuilder<'a, 'c> {
    pub fn new(in_progress: InProgress<'a, 'c>) -> Self {
        InProgressBuilder {
            in_progress: in_progress,
            builder: TermBuilder::new(),
        }
    }

    /// Build the terms from this builder and transact them against the current
    /// `InProgress`. This method _always_ returns the `InProgress` -- failure doesn't
    /// imply an automatic rollback.
    pub fn transact(self) -> (InProgress<'a, 'c>, Result<TxReport>)  {
        let mut in_progress = self.in_progress;
        let result = self.builder
                         .build()
                         .and_then(|(terms, _tempid_set)| {
                             in_progress.transact_entities(terms)
                         });
        (in_progress, result)
    }

    /// Transact the contents of the builder and commit the `InProgress`. If any
    /// step fails, roll back. Return the `TxReport`.
    pub fn commit(self) -> Result<TxReport> {
        let mut in_progress = self.in_progress;
        in_progress.transact_builder(self.builder)
                   .and_then(|report| {
                        in_progress.commit()?;
                        Ok(report)
                   })
    }
}

impl<'a, 'c> BuildTerms for InProgressBuilder<'a, 'c> {
    fn named_tempid<I>(&mut self, name: I) -> ValueRc<TempId> where I: Into<String> {
        self.builder.named_tempid(name)
    }

    fn describe_tempid(mut self, name: &str) -> EntityBuilder<InProgressBuilder<'a, 'c>> {
        let e = self.builder.named_tempid(name.to_string());
        self.describe(e)
    }

    fn describe<E>(self, entity: E) -> EntityBuilder<InProgressBuilder<'a, 'c>> where E: Into<EntityPlace<TypedValue>> {
        EntityBuilder {
            builder: self,
            entity: entity.into(),
        }
    }

    fn add<E, A, V>(&mut self, e: E, a: A, v: V) -> Result<()>
    where E: Into<EntityPlace<TypedValue>>,
          A: Into<AttributePlace>,
          V: Into<ValuePlace<TypedValue>> {
        self.builder.add(e, a, v)
    }

    fn retract<E, A, V>(&mut self, e: E, a: A, v: V) -> Result<()>
    where E: Into<EntityPlace<TypedValue>>,
          A: Into<AttributePlace>,
          V: Into<ValuePlace<TypedValue>> {
        self.builder.retract(e, a, v)
    }
}

impl<'a, 'c> EntityBuilder<InProgressBuilder<'a, 'c>> {
    /// Build the terms from this builder and transact them against the current
    /// `InProgress`. This method _always_ returns the `InProgress` -- failure doesn't
    /// imply an automatic rollback.
    pub fn transact(self) -> (InProgress<'a, 'c>, Result<TxReport>)  {
        self.finish().0.transact()
    }

    /// Transact the contents of the builder and commit the `InProgress`. If any
    /// step fails, roll back. Return the `TxReport`.
    pub fn commit(self) -> Result<TxReport> {
        self.finish().0.commit()
    }
}
