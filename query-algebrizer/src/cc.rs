// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code, unused_imports)]

extern crate mentat_core;
extern crate mentat_query;

use std::fmt::{
    Debug,
    Formatter,
    Result,
};
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

use self::mentat_core::{
    Attribute,
    Entid,
    Schema,
    TypedValue,
    ValueType,
};

use self::mentat_query::{
    IdentOrEntid,
    NamespacedKeyword,
    NonIntegerConstant,
    Pattern,
    PatternNonValuePlace,
    PatternValuePlace,
    PlainSymbol,
    SrcVar,
    Variable,
};

/// This enum models the fixed set of default tables we have -- two
/// tables and two views.
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum DatomsTable {
    Datoms,             // The non-fulltext datoms table.
    FulltextValues,     // The virtual table mapping IDs to strings.
    FulltextDatoms,     // The fulltext-datoms view.
    AllDatoms,          // Fulltext and non-fulltext datoms.
}

impl DatomsTable {
    pub fn name(&self) -> &'static str {
        match *self {
            DatomsTable::Datoms => "datoms",
            DatomsTable::FulltextValues => "fulltext_values",
            DatomsTable::FulltextDatoms => "fulltext_datoms",
            DatomsTable::AllDatoms => "all_datoms",
        }
    }
}

/// One of the named columns of our tables.
#[derive(PartialEq, Eq, Clone, Debug)]
pub enum DatomsColumn {
    Entity,
    Attribute,
    Value,
    Tx,
    ValueTypeTag,
}

impl DatomsColumn {
    pub fn as_str(&self) -> &'static str {
        use DatomsColumn::*;
        match *self {
            Entity => "e",
            Attribute => "a",
            Value => "v",
            Tx => "tx",
            ValueTypeTag => "value_type_tag",
        }
    }
}


/// A specific instance of a table within a query. E.g., "datoms123".
pub type TableAlias = String;

/// The association between a table and its alias. E.g., AllDatoms, "all_datoms123".
#[derive(PartialEq, Eq, Clone)]
pub struct SourceAlias(pub DatomsTable, pub TableAlias);

impl Debug for SourceAlias {
    fn fmt(&self, f: &mut Formatter) -> Result {
        write!(f, "SourceAlias({:?}, {})", self.0, self.1)
    }
}

/// A particular column of a particular aliased table. E.g., "datoms123", Attribute.
#[derive(PartialEq, Eq, Clone)]
pub struct QualifiedAlias(pub TableAlias, pub DatomsColumn);

impl Debug for QualifiedAlias {
    fn fmt(&self, f: &mut Formatter) -> Result {
        write!(f, "{}.{}", self.0, self.1.as_str())
    }
}

impl QualifiedAlias {
    fn for_type_tag(&self) -> QualifiedAlias {
        QualifiedAlias(self.0.clone(), DatomsColumn::ValueTypeTag)
    }

    #[inline]
    fn is_value(&self) -> bool {
        self.1 == DatomsColumn::Value
    }
}

/// A thing that's capable of aliasing a table name for us.
/// This exists so that we can obtain predictable names in tests.
pub type TableAliaser = Box<FnMut(DatomsTable) -> TableAlias>;

pub fn default_table_aliaser() -> TableAliaser {
    let mut i = -1;
    Box::new(move |table| {
        i += 1;
        format!("{}{:02}", table.name(), i)
    })
}

#[derive(PartialEq, Eq)]
pub enum ColumnConstraint {
    EqualsEntity(QualifiedAlias, Entid),
    EqualsValue(QualifiedAlias, TypedValue),
    EqualsColumn(QualifiedAlias, QualifiedAlias),
}

impl Debug for ColumnConstraint {
    fn fmt(&self, f: &mut Formatter) -> Result {
        use self::ColumnConstraint::*;
        match self {
            &EqualsEntity(ref qa, ref entid) => {
                write!(f, "{:?} = entity({:?})", qa, entid)
            }
            &EqualsValue(ref qa, ref typed_value) => {
                write!(f, "{:?} = value({:?})", qa, typed_value)
            }
            &EqualsColumn(ref qa1, ref qa2) => {
                write!(f, "{:?} = {:?}", qa1, qa2)
            }
        }
    }
}

/// A `ConjoiningClauses` (CC) is a collection of clauses that are combined with `JOIN`.
/// The topmost form in a query is a `ConjoiningClauses`.
///
/// - Ordinary pattern clauses turn into `FROM` parts and `WHERE` parts using `=`.
/// - Predicate clauses turn into the same, but with other functions.
/// - `not` turns into `NOT EXISTS` with `WHERE` clauses inside the subquery to
///   bind it to the outer variables, or adds simple `WHERE` clauses to the outer
///   clause.
/// - `not-join` is similar, but with explicit binding.
/// - `or` turns into a collection of `UNION`s inside a subquery, or a simple
///   alternation.
///   `or`'s documentation states that all clauses must include the same vars,
///   but that's an over-simplification: all clauses must refer to the external
///   unification vars.
///   The entire `UNION`-set is `JOIN`ed to any surrounding expressions per the `rule-vars`
///   clause, or the intersection of the vars in the two sides of the `JOIN`.
///
/// Not yet done:
/// - Function clauses with bindings turn into:
///   * Subqueries. Perhaps less efficient? Certainly clearer.
///   * Projection expressions, if only used for output.
///   * Inline expressions?
///---------------------------------------------------------------------------------------
pub struct ConjoiningClauses {
    /// `true` if this set of clauses cannot yield results in the context of the current schema.
    is_known_empty: bool,

    /// A function used to generate an alias for a table -- e.g., from "datoms" to "datoms123".
    aliaser: TableAliaser,

    /// A vector of source/alias pairs used to construct a SQL `FROM` list.
    pub from: Vec<SourceAlias>,

    /// A list of fragments that can be joined by `AND`.
    pub wheres: Vec<ColumnConstraint>,

    /// A map from var to qualified columns. Used to project.
    pub bindings: BTreeMap<Variable, Vec<QualifiedAlias>>,

    /// A map from var to type. Whenever a var maps unambiguously to two different types, it cannot
    /// yield results, so we don't represent that case here. If a var isn't present in the map, it
    /// means that its type is not known in advance.
    pub known_types: BTreeMap<Variable, ValueType>,

    /// A mapping, similar to `bindings`, but used to pull type tags out of the store at runtime.
    /// If a var isn't present in `known_types`, it should be present here.
    extracted_types: BTreeMap<Variable, QualifiedAlias>,
}

impl Debug for ConjoiningClauses {
    fn fmt(&self, fmt: &mut Formatter) -> Result {
        fmt.debug_struct("ConjoiningClauses")
            .field("is_known_empty", &self.is_known_empty)
            .field("from", &self.from)
            .field("wheres", &self.wheres)
            .field("bindings", &self.bindings)
            .field("known_types", &self.known_types)
            .field("extracted_types", &self.extracted_types)
            .finish()
    }
}

/// Basics.
impl Default for ConjoiningClauses {
    fn default() -> ConjoiningClauses {
        ConjoiningClauses {
            is_known_empty: false,
            aliaser: default_table_aliaser(),
            from: vec![],
            wheres: vec![],
            bindings: BTreeMap::new(),
            known_types: BTreeMap::new(),
            extracted_types: BTreeMap::new(),
        }
    }
}

impl ConjoiningClauses {
    pub fn bind_column_to_var(&mut self, table: TableAlias, column: DatomsColumn, var: Variable) {
        let alias = QualifiedAlias(table, column);

        // If this is a value, and we don't already know its type or where
        // to get its type, record that we can get it from this table.
        let needs_type_extraction =
            alias.is_value() &&
            !self.known_types.contains_key(&var) &&
            !self.extracted_types.contains_key(&var);

        if needs_type_extraction {
            self.extracted_types.insert(var.clone(), alias.for_type_tag());
        }
        self.bindings.entry(var).or_insert(vec![]).push(alias);
    }

    pub fn constrain_column_to_constant(&mut self, table: TableAlias, column: DatomsColumn, constant: TypedValue) {
        self.wheres.push(ColumnConstraint::EqualsValue(QualifiedAlias(table, column), constant))
    }

    pub fn constrain_column_to_entity(&mut self, table: TableAlias, column: DatomsColumn, entity: Entid) {
        self.wheres.push(ColumnConstraint::EqualsEntity(QualifiedAlias(table, column), entity))
    }

    pub fn constrain_attribute(&mut self, table: TableAlias, attribute: Entid) {
        self.constrain_column_to_entity(table, DatomsColumn::Attribute, attribute)
    }

    /// Constrains the var if there's no existing type.
    /// Returns `false` if it's impossible for this type to apply (because there's a conflicting
    /// type already known).
    fn constrain_var_to_type(&mut self, variable: Variable, this_type: ValueType) -> bool {
        // Is there an existing binding for this variable?
        let types_entry = self.known_types.entry(variable);
        match types_entry {
            // If so, the types must match.
            Entry::Occupied(entry) =>
                *entry.get() == this_type,
            // If not, record the one we just determined.
            Entry::Vacant(entry) => {
                entry.insert(this_type);
                true
            }
        }
    }

    /// Ensure that the given place has the correct types to be a tx-id.
    /// Right now this is mostly unimplemented: we fail hard if anything but a placeholder is
    /// present.
    fn constrain_to_tx(&mut self, tx: &PatternNonValuePlace) {
        match *tx {
            PatternNonValuePlace::Placeholder => (),
            _ => unimplemented!(),           // TODO
        }
    }

    /// Ensure that the given place can be an entity, and is congruent with existing types.
    /// This is used for `entity` and `attribute` places in a pattern.
    fn constrain_to_ref(&mut self, value: &PatternNonValuePlace) {
        // If it's a variable, record that it has the right type.
        // Ident or attribute resolution errors (the only other check we need to do) will be done
        // by the caller.
        if let &PatternNonValuePlace::Variable(ref v) = value {
            if !self.constrain_var_to_type(v.clone(), ValueType::Ref) {
                self.mark_known_empty("Couldn't constrain var to Ref.");
            }
        }
    }

    fn mark_known_empty(&mut self, why: &str) {
        self.is_known_empty = true;
        println!("{}", why);                   // TODO: proper logging.
    }

    fn entid_for_ident<'s, 'a>(&self, schema: &'s Schema, ident: &'a NamespacedKeyword) -> Option<Entid> {
        schema.get_entid(&ident)
    }

    fn table_for_attribute_and_value<'s, 'a>(&self, attribute: &'s Attribute, value: &'a PatternValuePlace) -> Option<DatomsTable> {
        if attribute.fulltext {
            match value {
                &PatternValuePlace::Placeholder =>
                    Some(DatomsTable::Datoms),            // We don't need the value.

                // TODO: an existing non-string binding can cause this pattern to fail.
                &PatternValuePlace::Variable(_) =>
                    Some(DatomsTable::AllDatoms),

                &PatternValuePlace::Constant(NonIntegerConstant::Text(_)) =>
                    Some(DatomsTable::AllDatoms),

                _ => {
                    // We can't succeed if there's a non-string constant value for a fulltext
                    // field.
                    None
                }
            }
        } else {
            Some(DatomsTable::Datoms)
        }
    }

    fn table_for_places<'s, 'a>(&self, schema: &'s Schema, attribute: &'a PatternNonValuePlace, value: &'a PatternValuePlace) -> Option<DatomsTable> {
        match attribute {
            // TODO: In a non-prepared context, check if a var is known by external binding, and
            // choose the table accordingly, as if it weren't a variable. #279.
            // TODO: In a prepared context, defer this decision until a second algebrizing phase.
            // #278.
            &PatternNonValuePlace::Placeholder | &PatternNonValuePlace::Variable(_) =>
                // If the value is known to be non-textual, we can simply use the regular datoms
                // table (TODO: and exclude on `index_fulltext`!).
                //
                // If the value is a placeholder too, then we can walk the non-value-joined view,
                // because we don't care about retrieving the fulltext value.
                //
                // If the value is a variable or string, we must use `all_datoms`, or do the join
                // ourselves, because we'll need to either extract or compare on the string.
                Some(
                    match value {
                        // TODO: see if the variable is projected, aggregated, or compared elsewhere in
                        // the query. If it's not, we don't need to use all_datoms here.
                        &PatternValuePlace::Variable(_) =>
                            DatomsTable::AllDatoms,       // TODO: check types.
                        &PatternValuePlace::Constant(NonIntegerConstant::Text(_)) =>
                            DatomsTable::AllDatoms,
                        _ =>
                            DatomsTable::Datoms,
                    }),
            &PatternNonValuePlace::Entid(id) =>
                schema.attribute_for_entid(id)
                      .and_then(|attribute| self.table_for_attribute_and_value(attribute, value)),
            &PatternNonValuePlace::Ident(ref kw) =>
                schema.attribute_for_ident(kw)
                      .and_then(|attribute| self.table_for_attribute_and_value(attribute, value)),
        }
    }

    /// Produce a (table, alias) pair to handle the provided pattern.
    /// This is a mutating method because it mutates the aliaser function!
    /// Note that if this function decides that a pattern cannot match, it will flip
    /// `is_known_empty`.
    fn alias_table<'s, 'a>(&mut self, schema: &'s Schema, pattern: &'a Pattern) -> Option<SourceAlias> {
        self.table_for_places(schema, &pattern.attribute, &pattern.value)
            .map(|table| SourceAlias(table, (self.aliaser)(table)))
    }

    fn get_attribute<'s, 'a>(&self, schema: &'s Schema, pattern: &'a Pattern) -> Option<&'s Attribute> {
        match pattern.attribute {
            PatternNonValuePlace::Entid(id) =>
                schema.attribute_for_entid(id),
            PatternNonValuePlace::Ident(ref kw) =>
                schema.attribute_for_ident(kw),
            _ =>
                None,
        }
    }

    fn get_value_type<'s, 'a>(&self, schema: &'s Schema, pattern: &'a Pattern) -> Option<ValueType> {
        self.get_attribute(schema, pattern).map(|x| x.value_type)
    }
}

/// Expansions.
impl ConjoiningClauses {

    /// Take the contents of `bindings` and generate inter-constraints for the appropriate
    /// columns into `wheres`.
    ///
    /// For example, a bindings map associating a var to three places in the query, like
    ///
    /// ```edn
    ///   {?foo [datoms12.e datoms13.v datoms14.e]}
    /// ```
    ///
    /// produces two additional constraints:
    ///
    /// ```example
    ///    datoms12.e = datoms13.v
    ///    datoms12.e = datoms14.e
    /// ```
    pub fn expand_bindings(&mut self) {
        for cols in self.bindings.values() {
            if cols.len() > 1 {
                let ref primary = cols[0];
                let secondaries = cols.iter().skip(1);
                for secondary in secondaries {
                    // TODO: if both primary and secondary are .v, should we make sure
                    // the type tag columns also match?
                    // We don't do so in the ClojureScript version.
                    self.wheres.push(ColumnConstraint::EqualsColumn(primary.clone(), secondary.clone()));
                }
            }
        }
    }

    /// When a CC has accumulated all patterns, generate value_type_tag entries in `wheres`
    /// to refine value types for which two things are true:
    ///
    /// - There are two or more different types with the same SQLite representation. E.g.,
    ///   ValueType::Boolean shares a representation with Integer and Ref.
    /// - There is no attribute constraint present in the CC.
    ///
    /// It's possible at this point for the space of acceptable type tags to not intersect: e.g.,
    /// for the query
    ///
    /// ```edn
    /// [:find ?x :where
    ///  [?x ?y true]
    ///  [?z ?y ?x]]
    /// ```
    ///
    /// where `?y` must simultaneously be a ref-typed attribute and a boolean-typed attribute. This
    /// function deduces that and calls `self.mark_known_empty`. #293.
    pub fn expand_type_tags(&mut self) {
        // TODO.
    }
}

/// Application.
impl ConjoiningClauses {

    /// Apply the constraints in the provided pattern to this CC.
    ///
    /// This is a single-pass process, which means it is naturally incomplete, failing to take into
    /// account all information spread across two patterns.
    ///
    /// If the constraints cannot be satisfied -- for example, if this pattern includes a numeric
    /// attribute and a string value -- then the `is_known_empty` field on the CC is flipped and
    /// the function returns.
    ///
    /// A pattern being impossible to satisfy isn't necessarily a bad thing -- this query might
    /// have branched clauses that apply to different knowledge bases, and might refer to
    /// vocabulary that isn't (yet) used in this one.
    ///
    /// Most of the work done by this function depends on the schema and ident maps in the DB. If
    /// these change, then any work done is invalid.
    ///
    /// There's a lot more we can do here and later by examining the
    /// attribute:
    ///
    /// - If it's unique, and we have patterns like
    ///
    ///     [?x :foo/unique 5] [?x :foo/unique ?y]
    ///
    ///   then we can either prove impossibility (the two objects must
    ///   be equal) or deduce identity and simplify the query.
    ///
    /// - The same, if it's cardinality-one and the entity is known.
    ///
    /// - If there's a value index on this attribute, we might want to
    ///   run this pattern early in the query.
    ///
    /// - A unique-valued attribute can sometimes be rewritten into an
    ///   existence subquery instead of a join.
    fn apply_pattern_clause_for_alias<'s>(&mut self, schema: &'s Schema, pattern: &Pattern, alias: &SourceAlias) {
        if self.is_known_empty {
            return;
        }

        // Process each place in turn, applying constraints.
        // Both `e` and `a` must be entities, which is equivalent here
        // to being typed as Ref.
        // Sorry for the duplication; Rust makes it a pain to abstract this.

        // The transaction part of a pattern must be an entid, variable, or placeholder.
        self.constrain_to_tx(&pattern.tx);
        self.constrain_to_ref(&pattern.entity);
        self.constrain_to_ref(&pattern.attribute);

        let ref col = alias.1;

        match pattern.entity {
            PatternNonValuePlace::Placeholder =>
                // Placeholders don't contribute any bindings, nor do
                // they constrain the query -- there's no need to produce
                // IS NOT NULL, because we don't store nulls in our schema.
                (),
            PatternNonValuePlace::Variable(ref v) =>
                self.bind_column_to_var(col.clone(), DatomsColumn::Entity, v.clone()),
            PatternNonValuePlace::Entid(entid) =>
                self.constrain_column_to_entity(col.clone(), DatomsColumn::Entity, entid),
            PatternNonValuePlace::Ident(ref ident) => {
                if let Some(entid) = self.entid_for_ident(schema, ident) {
                    self.constrain_column_to_entity(col.clone(), DatomsColumn::Entity, entid)
                } else {
                    // A resolution failure means we're done here.
                    self.mark_known_empty("Entity ident didn't resolve.");
                    return;
                }
            }
        }

        match pattern.attribute {
            PatternNonValuePlace::Placeholder =>
                (),
            PatternNonValuePlace::Variable(ref v) =>
                self.bind_column_to_var(col.clone(), DatomsColumn::Attribute, v.clone()),
            PatternNonValuePlace::Entid(entid) => {
                if !schema.is_attribute(entid) {
                    // Furthermore, that entid must resolve to an attribute. If it doesn't, this
                    // query is meaningless.
                    self.mark_known_empty("Attribute entid isn't an attribute.");
                    return;
                }
                self.constrain_column_to_entity(col.clone(), DatomsColumn::Attribute, entid)
            },
            PatternNonValuePlace::Ident(ref ident) => {
                if let Some(entid) = self.entid_for_ident(schema, ident) {
                    self.constrain_column_to_entity(col.clone(), DatomsColumn::Attribute, entid);

                    if !schema.is_attribute(entid) {
                        self.mark_known_empty("Attribute ident isn't an attribute.");
                        return;
                    }
                } else {
                    // A resolution failure means we're done here.
                    self.mark_known_empty("Attribute ident didn't resolve.");
                    return;
                }
            }
        }

        // Determine if the pattern's value type is known.
        // We do so by examining the value place and the attribute.
        // At this point it's possible that the type of the value is
        // inconsistent with the attribute; in that case this pattern
        // cannot return results, and we short-circuit.
        let value_type = self.get_value_type(schema, pattern);

        match pattern.value {
            PatternValuePlace::Placeholder =>
                (),
            PatternValuePlace::Variable(ref v) => {
                if let Some(this_type) = value_type {
                    // Wouldn't it be nice if we didn't need to clone in the found case?
                    // It doesn't matter too much: collisons won't be too frequent.
                    if !self.constrain_var_to_type(v.clone(), this_type) {
                        // The types don't match. This pattern cannot succeed.
                        self.mark_known_empty("Value types don't match.");
                        return;
                    }
                }

                self.bind_column_to_var(col.clone(), DatomsColumn::Value, v.clone());
            },
            PatternValuePlace::EntidOrInteger(i) =>
                // If we know the valueType, then we can determine whether this is an entid or an
                // integer. If we don't, then we must generate a more general query with a
                // value_type_tag.
                if let Some(ValueType::Ref) = value_type {
                    self.constrain_column_to_entity(col.clone(), DatomsColumn::Value, i);
                } else {
                    unimplemented!();
                },
            PatternValuePlace::IdentOrKeyword(ref kw) => {
                // If we know the valueType, then we can determine whether this is an ident or a
                // keyword. If we don't, then we must generate a more general query with a
                // value_type_tag.
                // We can also speculatively try to resolve it as an ident; if we fail, then we
                // know it can only return results if treated as a keyword, and we can treat it as
                // such.
                if let Some(ValueType::Ref) = value_type {
                    if let Some(entid) = self.entid_for_ident(schema, kw) {
                        self.constrain_column_to_entity(col.clone(), DatomsColumn::Value, entid)
                    } else {
                        // A resolution failure means we're done here: this attribute must have an
                        // entity value.
                        self.mark_known_empty("Value ident didn't resolve.");
                        return;
                    }
                } else {
                    unimplemented!();
                };
            },
            PatternValuePlace::Constant(ref c) => {
                // TODO: don't allocate.
                let typed_value = c.clone().into_typed_value();
                if !typed_value.is_congruent_with(value_type) {
                    // If the attribute and its value don't match, the pattern must fail.
                    self.mark_known_empty("Value constant not congruent with attribute type.");
                    return;
                }

                // TODO: if we don't know the type of the attribute because we don't know the
                // attribute, we can actually work backwards to the set of appropriate attributes
                // from the type of the value itself! #292.
                self.constrain_column_to_constant(col.clone(), DatomsColumn::Value, typed_value);
            },
        }

    }

    pub fn apply_pattern<'s, 'p>(&mut self, schema: &'s Schema, pattern: &'p Pattern) {
        // For now we only support the default source.
        match pattern.source {
            Some(SrcVar::DefaultSrc) | None => (),
            _ => unimplemented!(),
        };

        if let Some(alias) = self.alias_table(schema, &pattern) {
            self.apply_pattern_clause_for_alias(schema, pattern, &alias);
            self.from.push(alias);
        } else {
            // We didn't determine a table, likely because there was a mismatch
            // between an attribute and a value.
            // We know we cannot return a result, so we short-circuit here.
            self.mark_known_empty("Table aliaser couldn't determine a table.");
        }
    }
}

#[cfg(test)]
mod testing {
    use super::*;

    fn associate_ident(schema: &mut Schema, i: NamespacedKeyword, e: Entid) {
        schema.entid_map.insert(e, i.clone());
        schema.ident_map.insert(i.clone(), e);
    }

    fn add_attribute(schema: &mut Schema, e: Entid, a: Attribute) {
        schema.schema_map.insert(e, a);
    }

    #[test]
    fn test_unknown_ident() {
        let mut cc = ConjoiningClauses::default();
        let schema = Schema::default();

        cc.apply_pattern(&schema, &Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(Variable(PlainSymbol::new("?x"))),
            attribute: PatternNonValuePlace::Ident(NamespacedKeyword::new("foo", "bar")),
            value: PatternValuePlace::Constant(NonIntegerConstant::Boolean(true)),
            tx: PatternNonValuePlace::Placeholder,
        });

        assert!(cc.is_known_empty);
    }

    #[test]
    fn test_unknown_attribute() {
        let mut cc = ConjoiningClauses::default();
        let mut schema = Schema::default();

        associate_ident(&mut schema, NamespacedKeyword::new("foo", "bar"), 99);

        cc.apply_pattern(&schema, &Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(Variable(PlainSymbol::new("?x"))),
            attribute: PatternNonValuePlace::Ident(NamespacedKeyword::new("foo", "bar")),
            value: PatternValuePlace::Constant(NonIntegerConstant::Boolean(true)),
            tx: PatternNonValuePlace::Placeholder,
        });

        assert!(cc.is_known_empty);
    }

    #[test]
    fn test_apply_simple_pattern() {
        let mut cc = ConjoiningClauses::default();
        let mut schema = Schema::default();

        associate_ident(&mut schema, NamespacedKeyword::new("foo", "bar"), 99);
        add_attribute(&mut schema, 99, Attribute {
            value_type: ValueType::Boolean,
            ..Default::default()
        });

        let x = Variable(PlainSymbol::new("?x"));
        cc.apply_pattern(&schema, &Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            attribute: PatternNonValuePlace::Ident(NamespacedKeyword::new("foo", "bar")),
            value: PatternValuePlace::Constant(NonIntegerConstant::Boolean(true)),
            tx: PatternNonValuePlace::Placeholder,
        });

        // println!("{:#?}", cc);

        let d0_e = QualifiedAlias("datoms00".to_string(), DatomsColumn::Entity);
        let d0_a = QualifiedAlias("datoms00".to_string(), DatomsColumn::Attribute);
        let d0_v = QualifiedAlias("datoms00".to_string(), DatomsColumn::Value);

        // After this, we know a lot of things:
        assert!(!cc.is_known_empty);
        assert_eq!(cc.from, vec![SourceAlias(DatomsTable::Datoms, "datoms00".to_string())]);

        // ?x must be a ref.
        assert_eq!(cc.known_types.get(&x).unwrap(), &ValueType::Ref);

        // ?x is bound to datoms0.e.
        assert_eq!(cc.bindings.get(&x).unwrap(), &vec![d0_e.clone()]);

        // Our 'where' clauses are two:
        // - datoms0.a = 99
        // - datoms0.v = true
        // No need for a type tag constraint, because the attribute is known.
        assert_eq!(cc.wheres, vec![
                   ColumnConstraint::EqualsEntity(d0_a, 99),
                   ColumnConstraint::EqualsValue(d0_v, TypedValue::Boolean(true)),
        ]);
    }

    #[test]
    fn test_apply_unattributed_pattern() {
        let mut cc = ConjoiningClauses::default();
        let schema = Schema::default();

        let x = Variable(PlainSymbol::new("?x"));
        cc.apply_pattern(&schema, &Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            attribute: PatternNonValuePlace::Placeholder,
            value: PatternValuePlace::Constant(NonIntegerConstant::Boolean(true)),
            tx: PatternNonValuePlace::Placeholder,
        });

        // println!("{:#?}", cc);

        let d0_e = QualifiedAlias("datoms00".to_string(), DatomsColumn::Entity);
        let d0_v = QualifiedAlias("datoms00".to_string(), DatomsColumn::Value);

        assert!(!cc.is_known_empty);
        assert_eq!(cc.from, vec![SourceAlias(DatomsTable::Datoms, "datoms00".to_string())]);

        // ?x must be a ref.
        assert_eq!(cc.known_types.get(&x).unwrap(), &ValueType::Ref);

        // ?x is bound to datoms0.e.
        assert_eq!(cc.bindings.get(&x).unwrap(), &vec![d0_e.clone()]);

        // Our 'where' clauses are two:
        // - datoms0.v = true
        // - datoms0.value_type_tag = boolean
        // TODO: implement expand_type_tags.
        assert_eq!(cc.wheres, vec![
                   ColumnConstraint::EqualsValue(d0_v, TypedValue::Boolean(true)),
        ]);
    }

    #[test]
    fn test_apply_two_patterns() {
        let mut cc = ConjoiningClauses::default();
        let mut schema = Schema::default();

        associate_ident(&mut schema, NamespacedKeyword::new("foo", "bar"), 99);
        associate_ident(&mut schema, NamespacedKeyword::new("foo", "roz"), 98);
        add_attribute(&mut schema, 99, Attribute {
            value_type: ValueType::Boolean,
            ..Default::default()
        });
        add_attribute(&mut schema, 98, Attribute {
            value_type: ValueType::String,
            unique_identity: true,
            ..Default::default()
        });

        let x = Variable(PlainSymbol::new("?x"));
        let y = Variable(PlainSymbol::new("?y"));
        cc.apply_pattern(&schema, &Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            attribute: PatternNonValuePlace::Ident(NamespacedKeyword::new("foo", "roz")),
            value: PatternValuePlace::Constant(NonIntegerConstant::Text("idgoeshere".to_string())),
            tx: PatternNonValuePlace::Placeholder,
        });
        cc.apply_pattern(&schema, &Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            attribute: PatternNonValuePlace::Ident(NamespacedKeyword::new("foo", "bar")),
            value: PatternValuePlace::Variable(y.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });

        // Finally, expand bindings to get the overlaps for ?x.
        cc.expand_bindings();

        println!("{:#?}", cc);

        let d0_e = QualifiedAlias("datoms00".to_string(), DatomsColumn::Entity);
        let d0_a = QualifiedAlias("datoms00".to_string(), DatomsColumn::Attribute);
        let d0_v = QualifiedAlias("datoms00".to_string(), DatomsColumn::Value);
        let d1_e = QualifiedAlias("datoms01".to_string(), DatomsColumn::Entity);
        let d1_a = QualifiedAlias("datoms01".to_string(), DatomsColumn::Attribute);

        assert!(!cc.is_known_empty);
        assert_eq!(cc.from, vec![
                   SourceAlias(DatomsTable::Datoms, "datoms00".to_string()),
                   SourceAlias(DatomsTable::Datoms, "datoms01".to_string()),
        ]);

        // ?x must be a ref.
        assert_eq!(cc.known_types.get(&x).unwrap(), &ValueType::Ref);

        // ?x is bound to datoms0.e and datoms1.e.
        assert_eq!(cc.bindings.get(&x).unwrap(),
                   &vec![
                       d0_e.clone(),
                       d1_e.clone(),
                   ]);

        // Our 'where' clauses are four:
        // - datoms0.a = 98 (:foo/roz)
        // - datoms0.v = "idgoeshere"
        // - datoms1.a = 99 (:foo/bar)
        // - datoms1.e = datoms0.e
        assert_eq!(cc.wheres, vec![
                   ColumnConstraint::EqualsEntity(d0_a, 98),
                   ColumnConstraint::EqualsValue(d0_v, TypedValue::String("idgoeshere".to_string())),
                   ColumnConstraint::EqualsEntity(d1_a, 99),
                   ColumnConstraint::EqualsColumn(d0_e, d1_e),
        ]);
    }

}
