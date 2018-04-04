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
// See mentat_tx::entities::Entity and all of its child enums to see how complex this gets.
//
// A programmatic consumer doesn't want to build something that looks like:
//
//     Entity::AddOrRetract {
//         op: OpType::Add,
//         e: EntidOrLookupRefOrTempId::LookupRef(LookupRef {
//             a: Entid::Ident(NamespacedKeyword::new("test", "a1")),
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
// We probably need both, but this file provides the latter. Unfortunately, Entity -- the input to
// the transactor -- is intimately tied to EDN and to spanned values.

use mentat_core::{
    HasSchema,
    KnownEntid,
    NamespacedKeyword,
    TypedValue,
};

use mentat_core::intern_set::InternSet;
use mentat_core::util::Either;

use mentat_db::{
    TxReport,
};

use mentat_db::internal_types::{
    KnownEntidOr,
    TempIdHandle,
    Term,
    TermWithTempIds,
    TypedValueOr,
};

use mentat_tx::entities::{
    OpType,
    TempId,
};

use conn::{
    InProgress,
};

use errors::{
    ErrorKind,
    Result,
};

pub type Terms = (Vec<TermWithTempIds>, InternSet<TempId>);

pub struct TermBuilder {
    tempids: InternSet<TempId>,
    terms: Vec<TermWithTempIds>,
}

pub struct EntityBuilder<T: BuildTerms + Sized> {
    builder: T,
    entity: KnownEntidOr<TempIdHandle>,
}

pub trait BuildTerms where Self: Sized {
    fn named_tempid(&mut self, name: String) -> TempIdHandle;
    fn describe_tempid(self, name: &str) -> EntityBuilder<Self>;
    fn describe<E>(self, entity: E) -> EntityBuilder<Self> where E: IntoThing<KnownEntidOr<TempIdHandle>>;
    fn add<E, V>(&mut self, e: E, a: KnownEntid, v: V) -> Result<()>
    where E: IntoThing<KnownEntidOr<TempIdHandle>>,
          V: IntoThing<TypedValueOr<TempIdHandle>>;
    fn retract<E, V>(&mut self, e: E, a: KnownEntid, v: V) -> Result<()>
    where E: IntoThing<KnownEntidOr<TempIdHandle>>,
          V: IntoThing<TypedValueOr<TempIdHandle>>;
}

impl BuildTerms for TermBuilder {
    fn named_tempid(&mut self, name: String) -> TempIdHandle {
        self.tempids.intern(TempId::External(name))
    }

    fn describe_tempid(mut self, name: &str) -> EntityBuilder<Self> {
        let e = self.named_tempid(name.into());
        self.describe(e)
    }

    fn describe<E>(self, entity: E) -> EntityBuilder<Self> where E: IntoThing<KnownEntidOr<TempIdHandle>> {
        EntityBuilder {
            builder: self,
            entity: entity.into_thing(),
        }
    }

    fn add<E, V>(&mut self, e: E, a: KnownEntid, v: V) -> Result<()>
    where E: IntoThing<KnownEntidOr<TempIdHandle>>,
          V: IntoThing<TypedValueOr<TempIdHandle>> {
        let e = e.into_thing();
        let v = v.into_thing();
        self.terms.push(Term::AddOrRetract(OpType::Add, e, a.into(), v));
        Ok(())
    }

    fn retract<E, V>(&mut self, e: E, a: KnownEntid, v: V) -> Result<()>
    where E: IntoThing<KnownEntidOr<TempIdHandle>>,
          V: IntoThing<TypedValueOr<TempIdHandle>> {
        let e = e.into_thing();
        let v = v.into_thing();
        self.terms.push(Term::AddOrRetract(OpType::Retract, e, a.into(), v));
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
    pub fn numbered_tempid(&mut self, id: i64) -> TempIdHandle {
        self.tempids.intern(TempId::Internal(id))
    }
}

impl<T> EntityBuilder<T> where T: BuildTerms {
    pub fn finish(self) -> (T, KnownEntidOr<TempIdHandle>) {
        (self.builder, self.entity)
    }

    pub fn add<V>(&mut self, a: KnownEntid, v: V) -> Result<()>
    where V: IntoThing<TypedValueOr<TempIdHandle>> {
        self.builder.add(self.entity.clone(), a, v)
    }

    pub fn retract<V>(&mut self, a: KnownEntid, v: V) -> Result<()>
    where V: IntoThing<TypedValueOr<TempIdHandle>> {
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
                         .and_then(|(terms, tempid_set)| {
                             in_progress.transact_terms(terms, tempid_set)
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
    fn named_tempid(&mut self, name: String) -> TempIdHandle {
        self.builder.named_tempid(name)
    }

    fn describe_tempid(mut self, name: &str) -> EntityBuilder<InProgressBuilder<'a, 'c>> {
        let e = self.builder.named_tempid(name.into());
        self.describe(e)
    }

    fn describe<E>(self, entity: E) -> EntityBuilder<InProgressBuilder<'a, 'c>> where E: IntoThing<KnownEntidOr<TempIdHandle>> {
        EntityBuilder {
            builder: self,
            entity: entity.into_thing(),
        }
    }

    fn add<E, V>(&mut self, e: E, a: KnownEntid, v: V) -> Result<()>
    where E: IntoThing<KnownEntidOr<TempIdHandle>>,
          V: IntoThing<TypedValueOr<TempIdHandle>> {
        self.builder.add(e, a, v)
    }

    fn retract<E, V>(&mut self, e: E, a: KnownEntid, v: V) -> Result<()>
    where E: IntoThing<KnownEntidOr<TempIdHandle>>,
          V: IntoThing<TypedValueOr<TempIdHandle>> {
        self.builder.retract(e, a, v)
    }
}

impl<'a, 'c> InProgressBuilder<'a, 'c> {
    pub fn add_kw<E, V>(&mut self, e: E, a: &NamespacedKeyword, v: V) -> Result<()>
    where E: IntoThing<KnownEntidOr<TempIdHandle>>,
          V: IntoThing<TypedValueOr<TempIdHandle>> {
        let (attribute, value) = self.extract_kw_value(a, v.into_thing())?;
        self.add(e, attribute, value)
    }

    pub fn retract_kw<E, V>(&mut self, e: E, a: &NamespacedKeyword, v: V) -> Result<()>
    where E: IntoThing<KnownEntidOr<TempIdHandle>>,
          V: IntoThing<TypedValueOr<TempIdHandle>> {
        let (attribute, value) = self.extract_kw_value(a, v.into_thing())?;
        self.retract(e, attribute, value)
    }

    fn extract_kw_value(&mut self, a: &NamespacedKeyword, v: TypedValueOr<TempIdHandle>) -> Result<(KnownEntid, TypedValueOr<TempIdHandle>)> {
        let attribute: KnownEntid;
        if let Some((attr, aa)) = self.in_progress.attribute_for_ident(a) {
            if let Either::Left(ref tv) = v {
                let provided = tv.value_type();
                let expected = attr.value_type;
                if provided != expected {
                    bail!(ErrorKind::ValueTypeMismatch(provided, expected));
                }
            }
            attribute = aa;
        } else {
            bail!(ErrorKind::UnknownAttribute(a.to_string()));
        }
        Ok((attribute, v))
    }
}

impl<'a, 'c> EntityBuilder<InProgressBuilder<'a, 'c>> {
    pub fn add_kw<V>(&mut self, a: &NamespacedKeyword, v: V) -> Result<()>
    where V: IntoThing<TypedValueOr<TempIdHandle>> {
        self.builder.add_kw(self.entity.clone(), a, v)
    }

    pub fn retract_kw<V>(&mut self, a: &NamespacedKeyword, v: V) -> Result<()>
    where V: IntoThing<TypedValueOr<TempIdHandle>> {
        self.builder.retract_kw(self.entity.clone(), a, v)
    }

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

// Can't implement Into for Rc<T>.
pub trait IntoThing<T>: Sized {
    fn into_thing(self) -> T;
}

pub trait FromThing<T> {
    fn from_thing(v: T) -> Self;
}

impl<T> FromThing<T> for T {
    fn from_thing(v: T) -> T {
        v
    }
}

impl<I, F> IntoThing<I> for F where I: FromThing<F> {
    fn into_thing(self) -> I {
        I::from_thing(self)
    }
}

impl<'a> FromThing<&'a TempIdHandle> for TypedValueOr<TempIdHandle> {
    fn from_thing(v: &'a TempIdHandle) -> Self {
        Either::Right(v.clone())
    }
}

impl FromThing<TempIdHandle> for TypedValueOr<TempIdHandle> {
    fn from_thing(v: TempIdHandle) -> Self {
        Either::Right(v)
    }
}

impl FromThing<TypedValue> for TypedValueOr<TempIdHandle> {
    fn from_thing(v: TypedValue) -> Self {
        Either::Left(v)
    }
}

impl FromThing<TempIdHandle> for KnownEntidOr<TempIdHandle> {
    fn from_thing(v: TempIdHandle) -> Self {
        Either::Right(v)
    }
}

impl<'a> FromThing<&'a KnownEntid> for KnownEntidOr<TempIdHandle> {
    fn from_thing(v: &'a KnownEntid) -> Self {
        Either::Left(v.clone())
    }
}

impl FromThing<KnownEntid> for KnownEntidOr<TempIdHandle> {
    fn from_thing(v: KnownEntid) -> Self {
        Either::Left(v)
    }
}

impl FromThing<KnownEntid> for TypedValueOr<TempIdHandle> {
    fn from_thing(v: KnownEntid) -> Self {
        Either::Left(v.into())
    }
}

#[cfg(test)]
mod testing {
    extern crate mentat_db;

    use errors::{
        Error,
        ErrorKind,
    };

    // For matching inside a test.
    use mentat_db::ErrorKind::{
        UnrecognizedEntid,
    };

    use ::{
        Conn,
        Entid,
        HasSchema,
        Queryable,
        TypedValue,
        TxReport,
    };

    use super::*;

    // In reality we expect the store to hand these out safely.
    fn fake_known_entid(e: Entid) -> KnownEntid {
        KnownEntid(e)
    }

    #[test]
    fn test_entity_builder_bogus_entids() {
        let mut builder = TermBuilder::new();
        let e = builder.named_tempid("x".into());
        let a1 = fake_known_entid(37);    // :db/doc
        let a2 = fake_known_entid(999);
        let v = TypedValue::typed_string("Some attribute");
        let ve = fake_known_entid(12345);

        builder.add(e.clone(), a1, v).expect("add succeeded");
        builder.add(e.clone(), a2, e.clone()).expect("add succeeded, even though it's meaningless");
        builder.add(e.clone(), a2, ve).expect("add succeeded, even though it's meaningless");
        let (terms, tempids) = builder.build().expect("build succeeded");

        assert_eq!(tempids.len(), 1);
        assert_eq!(terms.len(), 3);     // TODO: check the contents?

        // Now try to add them to a real store.
        let mut sqlite = mentat_db::db::new_connection("").unwrap();
        let mut conn = Conn::connect(&mut sqlite).unwrap();
        let mut in_progress = conn.begin_transaction(&mut sqlite).expect("begun successfully");

        // This should fail: unrecognized entid.
        if let Err(Error(ErrorKind::DbError(UnrecognizedEntid(e)), _)) = in_progress.transact_terms(terms, tempids) {
            assert_eq!(e, 999);
        } else {
            panic!("Should have rejected the entid.");
        }
    }

    #[test]
    fn test_in_progress_builder() {
        let mut sqlite = mentat_db::db::new_connection("").unwrap();
        let mut conn = Conn::connect(&mut sqlite).unwrap();

        // Give ourselves a schema to work with!
        conn.transact(&mut sqlite, r#"[
            [:db/add "o" :db/ident :foo/one]
            [:db/add "o" :db/valueType :db.type/long]
            [:db/add "o" :db/cardinality :db.cardinality/one]
            [:db/add "m" :db/ident :foo/many]
            [:db/add "m" :db/valueType :db.type/string]
            [:db/add "m" :db/cardinality :db.cardinality/many]
            [:db/add "r" :db/ident :foo/ref]
            [:db/add "r" :db/valueType :db.type/ref]
            [:db/add "r" :db/cardinality :db.cardinality/one]
        ]"#).unwrap();

        let in_progress = conn.begin_transaction(&mut sqlite).expect("begun successfully");

        // We can use this or not!
        let a_many = in_progress.get_entid(&kw!(:foo/many)).expect(":foo/many");

        let mut builder = in_progress.builder();
        let e_x = builder.named_tempid("x".into());
        let v_many_1 = TypedValue::typed_string("Some text");
        let v_many_2 = TypedValue::typed_string("Other text");
        builder.add_kw(e_x.clone(), &kw!(:foo/many), v_many_1).expect("add succeeded");
        builder.add(e_x.clone(), a_many, v_many_2).expect("add succeeded");
        builder.commit().expect("commit succeeded");
    }

    #[test]
    fn test_entity_builder() {
        let mut sqlite = mentat_db::db::new_connection("").unwrap();
        let mut conn = Conn::connect(&mut sqlite).unwrap();

        let foo_one = kw!(:foo/one);
        let foo_many = kw!(:foo/many);
        let foo_ref = kw!(:foo/ref);
        let report: TxReport;

        // Give ourselves a schema to work with!
        // Scoped borrow of conn.
        {
            conn.transact(&mut sqlite, r#"[
                [:db/add "o" :db/ident :foo/one]
                [:db/add "o" :db/valueType :db.type/long]
                [:db/add "o" :db/cardinality :db.cardinality/one]
                [:db/add "m" :db/ident :foo/many]
                [:db/add "m" :db/valueType :db.type/string]
                [:db/add "m" :db/cardinality :db.cardinality/many]
                [:db/add "r" :db/ident :foo/ref]
                [:db/add "r" :db/valueType :db.type/ref]
                [:db/add "r" :db/cardinality :db.cardinality/one]
            ]"#).unwrap();

            let mut in_progress = conn.begin_transaction(&mut sqlite).expect("begun successfully");

            // Scoped borrow of in_progress.
            {
                let mut builder = TermBuilder::new();
                let e_x = builder.named_tempid("x".into());
                let e_y = builder.named_tempid("y".into());
                let a_ref = in_progress.get_entid(&foo_ref).expect(":foo/ref");
                let a_one = in_progress.get_entid(&foo_one).expect(":foo/one");
                let a_many = in_progress.get_entid(&foo_many).expect(":foo/many");
                let v_many_1 = TypedValue::typed_string("Some text");
                let v_many_2 = TypedValue::typed_string("Other text");
                let v_long: TypedValue = 123.into();

                builder.add(e_x.clone(), a_many, v_many_1).expect("add succeeded");
                builder.add(e_x.clone(), a_many, v_many_2).expect("add succeeded");
                builder.add(e_y.clone(), a_ref, e_x.clone()).expect("add succeeded");
                builder.add(e_x.clone(), a_one, v_long).expect("add succeeded");

                let (terms, tempids) = builder.build().expect("build succeeded");

                assert_eq!(tempids.len(), 2);
                assert_eq!(terms.len(), 4);

                report = in_progress.transact_terms(terms, tempids).expect("add succeeded");
                let x = report.tempids.get("x").expect("our tempid has an ID");
                let y = report.tempids.get("y").expect("our tempid has an ID");
                assert_eq!(in_progress.lookup_value_for_attribute(*y, &foo_ref).expect("lookup succeeded"),
                           Some(TypedValue::Ref(*x)));
                assert_eq!(in_progress.lookup_value_for_attribute(*x, &foo_one).expect("lookup succeeded"),
                           Some(TypedValue::Long(123)));
            }

            in_progress.commit().expect("commit succeeded");
        }

        // It's all still there after the commit.
        let x = report.tempids.get("x").expect("our tempid has an ID");
        let y = report.tempids.get("y").expect("our tempid has an ID");
        assert_eq!(conn.lookup_value_for_attribute(&mut sqlite, *y, &foo_ref).expect("lookup succeeded"),
                   Some(TypedValue::Ref(*x)));
    }
}
