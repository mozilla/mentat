// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::{
    BTreeMap,
    BTreeSet,
    HashSet,
};

use std::collections::btree_map::Entry;

use std::fmt::{
    Debug,
    Formatter,
};

use mentat_core::{
    Attribute,
    Entid,
    Schema,
    TypedValue,
    ValueType,
};

use mentat_query::{
    NamespacedKeyword,
    NonIntegerConstant,
    Pattern,
    PatternNonValuePlace,
    PatternValuePlace,
    Variable,
    WhereClause,
};

use errors::{
    Result,
};

use types::{
    ColumnConstraint,
    ColumnIntersection,
    DatomsColumn,
    DatomsTable,
    EmptyBecause,
    QualifiedAlias,
    QueryValue,
    SourceAlias,
    TableAlias,
};

mod or;
mod pattern;
mod predicate;
mod resolve;

use validate::validate_or_join;

// We do this a lot for errors.
trait RcCloned<T> {
    fn cloned(&self) -> T;
}

impl<T: Clone> RcCloned<T> for ::std::rc::Rc<T> {
    fn cloned(&self) -> T {
        self.as_ref().clone()
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

fn unit_type_set(t: ValueType) -> HashSet<ValueType> {
    let mut s = HashSet::with_capacity(1);
    s.insert(t);
    s
}

trait Contains<K, T> {
    fn when_contains<F: FnOnce() -> T>(&self, k: &K, f: F) -> Option<T>;
}

trait Intersection<K> {
    fn with_intersected_keys(&self, ks: &BTreeSet<K>) -> Self;
}

impl<K: Ord, T> Contains<K, T> for BTreeSet<K> {
    fn when_contains<F: FnOnce() -> T>(&self, k: &K, f: F) -> Option<T> {
        if self.contains(k) {
            Some(f())
        } else {
            None
        }
    }
}

impl<K: Clone + Ord, V: Clone> Intersection<K> for BTreeMap<K, V> {
    /// Return a clone of the map with only keys that are present in `ks`.
    fn with_intersected_keys(&self, ks: &BTreeSet<K>) -> Self {
        self.iter()
            .filter_map(|(k, v)| ks.when_contains(k, || (k.clone(), v.clone())))
            .collect()
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
    pub is_known_empty: bool,
    pub empty_because: Option<EmptyBecause>,

    /// A function used to generate an alias for a table -- e.g., from "datoms" to "datoms123".
    aliaser: TableAliaser,

    /// A vector of source/alias pairs used to construct a SQL `FROM` list.
    pub from: Vec<SourceAlias>,

    /// A list of fragments that can be joined by `AND`.
    pub wheres: ColumnIntersection,

    /// A map from var to qualified columns. Used to project.
    pub column_bindings: BTreeMap<Variable, Vec<QualifiedAlias>>,

    /// A list of variables mentioned in the enclosing query's :in clause. These must all be bound
    /// before the query can be executed. TODO: clarify what this means for nested CCs.
    pub input_variables: BTreeSet<Variable>,

    /// In some situations -- e.g., when a query is being run only once -- we know in advance the
    /// values bound to some or all variables. These can be substituted directly when the query is
    /// algebrized.
    ///
    /// Value bindings must agree with `known_types`. If you write a query like
    /// ```edn
    /// [:find ?x :in $ ?val :where [?x :foo/int ?val]]
    /// ```
    ///
    /// and for `?val` provide `TypedValue::String("foo".to_string())`, the query will be known at
    /// algebrizing time to be empty.
    value_bindings: BTreeMap<Variable, TypedValue>,

    /// A map from var to type. Whenever a var maps unambiguously to two different types, it cannot
    /// yield results, so we don't represent that case here. If a var isn't present in the map, it
    /// means that its type is not known in advance.
    pub known_types: BTreeMap<Variable, HashSet<ValueType>>,

    /// A mapping, similar to `column_bindings`, but used to pull type tags out of the store at runtime.
    /// If a var isn't present in `known_types`, it should be present here.
    extracted_types: BTreeMap<Variable, QualifiedAlias>,
}

impl Debug for ConjoiningClauses {
    fn fmt(&self, fmt: &mut Formatter) -> ::std::fmt::Result {
        fmt.debug_struct("ConjoiningClauses")
            .field("is_known_empty", &self.is_known_empty)
            .field("from", &self.from)
            .field("wheres", &self.wheres)
            .field("column_bindings", &self.column_bindings)
            .field("input_variables", &self.input_variables)
            .field("value_bindings", &self.value_bindings)
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
            empty_because: None,
            aliaser: default_table_aliaser(),
            from: vec![],
            wheres: ColumnIntersection::default(),
            input_variables: BTreeSet::new(),
            column_bindings: BTreeMap::new(),
            value_bindings: BTreeMap::new(),
            known_types: BTreeMap::new(),
            extracted_types: BTreeMap::new(),
        }
    }
}

/// Cloning.
impl ConjoiningClauses {
    fn make_receptacle(&self) -> ConjoiningClauses {
        let mut concrete = ConjoiningClauses::default();
        concrete.is_known_empty = self.is_known_empty;
        concrete.empty_because = self.empty_because.clone();

        concrete.input_variables = self.input_variables.clone();
        concrete.value_bindings = self.value_bindings.clone();
        concrete.known_types = self.known_types.clone();
        concrete.extracted_types = self.extracted_types.clone();

        concrete
    }

    /// Make a new CC populated with the relevant variable associations in this CC.
    /// Note that the CC's table aliaser is not yet usable. That's not a problem for templating for
    /// simple `or`.
    fn use_as_template(&self, vars: &BTreeSet<Variable>) -> ConjoiningClauses {
        let mut template = ConjoiningClauses::default();
        template.is_known_empty = self.is_known_empty;
        template.empty_because = self.empty_because.clone();

        template.input_variables = self.input_variables.intersection(vars).cloned().collect();
        template.value_bindings = self.value_bindings.with_intersected_keys(&vars);
        template.known_types = self.known_types.with_intersected_keys(&vars);
        template.extracted_types = self.extracted_types.with_intersected_keys(&vars);

        template
    }
}

impl ConjoiningClauses {
    #[allow(dead_code)]
    fn with_value_bindings(bindings: BTreeMap<Variable, TypedValue>) -> ConjoiningClauses {
        let mut cc = ConjoiningClauses {
            value_bindings: bindings,
            ..Default::default()
        };

        // Pre-fill our type mappings with the types of the input bindings.
        cc.known_types
          .extend(cc.value_bindings
                    .iter()
                    .map(|(k, v)| (k.clone(), unit_type_set(v.value_type()))));
        cc
    }
}

impl ConjoiningClauses {
    fn bound_value(&self, var: &Variable) -> Option<TypedValue> {
        self.value_bindings.get(var).cloned()
    }

    /// Return a single `ValueType` if the given variable is known to have a precise type.
    /// Returns `None` if the type of the variable is unknown.
    /// Returns `None` if the type of the variable is known but not precise -- "double
    /// or integer" isn't good enough.
    pub fn known_type(&self, var: &Variable) -> Option<ValueType> {
        match self.known_types.get(var) {
            Some(types) if types.len() == 1 => types.iter().next().cloned(),
            _ => None,
        }
    }

    pub fn bind_column_to_var(&mut self, schema: &Schema, table: TableAlias, column: DatomsColumn, var: Variable) {
        // Do we have an external binding for this?
        if let Some(bound_val) = self.bound_value(&var) {
            // Great! Use that instead.
            // We expect callers to do things like bind keywords here; we need to translate these
            // before they hit our constraints.
            // TODO: recognize when the valueType might be a ref and also translate entids there.
            if column == DatomsColumn::Value {
                self.constrain_column_to_constant(table, column, bound_val);
            } else {
                match bound_val {
                    TypedValue::Keyword(ref kw) => {
                        if let Some(entid) = self.entid_for_ident(schema, kw) {
                            self.constrain_column_to_entity(table, column, entid);
                        } else {
                            // Impossible.
                            // For attributes this shouldn't occur, because we check the binding in
                            // `table_for_places`/`alias_table`, and if it didn't resolve to a valid
                            // attribute then we should have already marked the pattern as empty.
                            self.mark_known_empty(EmptyBecause::UnresolvedIdent(kw.cloned()));
                        }
                    },
                    TypedValue::Ref(entid) => {
                        self.constrain_column_to_entity(table, column, entid);
                    },
                    _ => {
                        // One can't bind an e, a, or tx to something other than an entity.
                        self.mark_known_empty(EmptyBecause::InvalidBinding(column, bound_val));
                    },
                }
            }

            return;
        }

        // Will we have an external binding for this?
        // If so, we don't need to extract its type. We'll know it later.
        let late_binding = self.input_variables.contains(&var);

        // If this is a value, and we don't already know its type or where
        // to get its type, record that we can get it from this table.
        let needs_type_extraction =
            !late_binding &&                           // Never need to extract for bound vars.
            column == DatomsColumn::Value &&           // Never need to extract types for refs.
            self.known_type(&var).is_none() &&         // Don't need to extract if we know a single type.
            !self.extracted_types.contains_key(&var);  // We're already extracting the type.

        let alias = QualifiedAlias(table, column);

        // If we subsequently find out its type, we'll remove this later -- see
        // the removal in `constrain_var_to_type`.
        if needs_type_extraction {
            self.extracted_types.insert(var.clone(), alias.for_type_tag());
        }
        self.column_bindings.entry(var).or_insert(vec![]).push(alias);
    }

    pub fn constrain_column_to_constant(&mut self, table: TableAlias, column: DatomsColumn, constant: TypedValue) {
        self.wheres.add_intersection(ColumnConstraint::Equals(QualifiedAlias(table, column), QueryValue::TypedValue(constant)))
    }

    pub fn constrain_column_to_entity(&mut self, table: TableAlias, column: DatomsColumn, entity: Entid) {
        self.wheres.add_intersection(ColumnConstraint::Equals(QualifiedAlias(table, column), QueryValue::Entid(entity)))
    }

    pub fn constrain_attribute(&mut self, table: TableAlias, attribute: Entid) {
        self.constrain_column_to_entity(table, DatomsColumn::Attribute, attribute)
    }

    pub fn constrain_value_to_numeric(&mut self, table: TableAlias, value: i64) {
        self.wheres.add_intersection(ColumnConstraint::Equals(
            QualifiedAlias(table, DatomsColumn::Value),
            QueryValue::PrimitiveLong(value)))
    }

    /// Mark the given value as one of the set of numeric types.
    fn constrain_var_to_numeric(&mut self, variable: Variable) {
        let mut numeric_types = HashSet::with_capacity(2);
        numeric_types.insert(ValueType::Double);
        numeric_types.insert(ValueType::Long);

        self.narrow_types_for_var(variable, numeric_types);
    }

    /// Constrains the var if there's no existing type.
    /// Marks as known-empty if it's impossible for this type to apply because there's a conflicting
    /// type already known.
    fn constrain_var_to_type(&mut self, variable: Variable, this_type: ValueType) {
        // Is there an existing mapping for this variable?
        // Any known inputs have already been added to known_types, and so if they conflict we'll
        // spot it here.
        if let Some(existing) = self.known_types.insert(variable.clone(), unit_type_set(this_type)) {
            // There was an existing mapping. Does this type match?
            if !existing.contains(&this_type) {
                self.mark_known_empty(EmptyBecause::TypeMismatch(variable, existing, this_type));
            }
        }
    }

    /// Like `constrain_var_to_type` but in reverse: this expands the set of types
    /// with which a variable is associated.
    ///
    /// N.B.,: if we ever call `broaden_types` after `is_known_empty` has been set, we might
    /// actually move from a state in which a variable can have no type to one that can
    /// yield results! We never do so at present -- we carefully set-union types before we
    /// set-intersect them -- but this is worth bearing in mind.
    pub fn broaden_types(&mut self, additional_types: BTreeMap<Variable, HashSet<ValueType>>) {
        for (var, new_types) in additional_types {
            match self.known_types.entry(var) {
                Entry::Vacant(e) => {
                    if new_types.len() == 1 {
                        self.extracted_types.remove(e.key());
                    }
                    e.insert(new_types);
                },
                Entry::Occupied(mut e) => {
                    if e.get().is_empty() && self.is_known_empty {
                        panic!("Uh oh: we failed this pattern, probably because {:?} couldn't match, but now we're broadening its type.",
                               e.get());
                    }
                    e.get_mut().extend(new_types.into_iter());
                },
            }
        }
    }

    /// Restrict the known types for `var` to intersect with `types`.
    /// If no types are already known -- `var` could have any type -- then this is equivalent to
    /// simply setting the known types to `types`.
    /// If the known types don't intersect with `types`, mark the pattern as known-empty.
    fn narrow_types_for_var(&mut self, var: Variable, types: HashSet<ValueType>) {
        if types.is_empty() {
            // We hope this never occurs; we should catch this case earlier.
            self.mark_known_empty(EmptyBecause::NoValidTypes(var));
            return;
        }

        // We can't mutate `empty_because` while we're working with the `Entry`, so do this instead.
        let mut empty_because: Option<EmptyBecause> = None;
        match self.known_types.entry(var) {
            Entry::Vacant(e) => {
                e.insert(types);
            },
            Entry::Occupied(mut e) => {
                // TODO: we shouldn't need to clone here.
                let intersected: HashSet<_> = types.intersection(e.get()).cloned().collect();
                if intersected.is_empty() {
                    let mismatching_type = types.iter().next().unwrap().clone();
                    let reason = EmptyBecause::TypeMismatch(e.key().clone(),
                                                            e.get().clone(),
                                                            mismatching_type);
                    empty_because = Some(reason);
                }
                // Always insert, even if it's empty!
                e.insert(intersected);
            },
        }

        if let Some(e) = empty_because {
            self.mark_known_empty(e);
        }
    }

    /// Restrict the sets of types for the provided vars to the provided types.
    /// See `narrow_types_for_var`.
    pub fn narrow_types(&mut self, additional_types: BTreeMap<Variable, HashSet<ValueType>>) {
        if additional_types.is_empty() {
            return;
        }
        for (var, new_types) in additional_types {
            self.narrow_types_for_var(var, new_types);
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
            self.constrain_var_to_type(v.clone(), ValueType::Ref)
        }
    }

    fn mark_known_empty(&mut self, why: EmptyBecause) {
        self.is_known_empty = true;
        if self.empty_because.is_some() {
            return;
        }
        println!("CC known empty: {:?}.", &why);                   // TODO: proper logging.
        self.empty_because = Some(why);
    }

    fn entid_for_ident<'s, 'a>(&self, schema: &'s Schema, ident: &'a NamespacedKeyword) -> Option<Entid> {
        schema.get_entid(&ident)
    }

    fn table_for_attribute_and_value<'s, 'a>(&self, attribute: &'s Attribute, value: &'a PatternValuePlace) -> ::std::result::Result<DatomsTable, EmptyBecause> {
        if attribute.fulltext {
            match value {
                &PatternValuePlace::Placeholder =>
                    Ok(DatomsTable::Datoms),            // We don't need the value.

                // TODO: an existing non-string binding can cause this pattern to fail.
                &PatternValuePlace::Variable(_) =>
                    Ok(DatomsTable::AllDatoms),

                &PatternValuePlace::Constant(NonIntegerConstant::Text(_)) =>
                    Ok(DatomsTable::AllDatoms),

                _ => {
                    // We can't succeed if there's a non-string constant value for a fulltext
                    // field.
                    Err(EmptyBecause::NonStringFulltextValue)
                },
            }
        } else {
            Ok(DatomsTable::Datoms)
        }
    }

    fn table_for_unknown_attribute<'s, 'a>(&self, value: &'a PatternValuePlace) -> ::std::result::Result<DatomsTable, EmptyBecause> {
        // If the value is known to be non-textual, we can simply use the regular datoms
        // table (TODO: and exclude on `index_fulltext`!).
        //
        // If the value is a placeholder too, then we can walk the non-value-joined view,
        // because we don't care about retrieving the fulltext value.
        //
        // If the value is a variable or string, we must use `all_datoms`, or do the join
        // ourselves, because we'll need to either extract or compare on the string.
        Ok(
            match value {
                // TODO: see if the variable is projected, aggregated, or compared elsewhere in
                // the query. If it's not, we don't need to use all_datoms here.
                &PatternValuePlace::Variable(ref v) => {
                    // Do we know that this variable can't be a string? If so, we don't need
                    // AllDatoms. None or String means it could be or definitely is.
                    match self.known_types.get(v).map(|types| types.contains(&ValueType::String)) {
                        Some(false) => DatomsTable::Datoms,
                        _           => DatomsTable::AllDatoms,
                    }
                }
                &PatternValuePlace::Constant(NonIntegerConstant::Text(_)) =>
                    DatomsTable::AllDatoms,
                _ =>
                    DatomsTable::Datoms,
            })
    }

    /// Decide which table to use for the provided attribute and value.
    /// If the attribute input or value binding doesn't name an attribute, or doesn't name an
    /// attribute that is congruent with the supplied value, we return an `EmptyBecause`.
    /// The caller is responsible for marking the CC as known-empty if this is a fatal failure.
    fn table_for_places<'s, 'a>(&self, schema: &'s Schema, attribute: &'a PatternNonValuePlace, value: &'a PatternValuePlace) -> ::std::result::Result<DatomsTable, EmptyBecause> {
        match attribute {
            &PatternNonValuePlace::Ident(ref kw) =>
                schema.attribute_for_ident(kw)
                      .ok_or_else(|| EmptyBecause::InvalidAttributeIdent(kw.cloned()))
                      .and_then(|attribute| self.table_for_attribute_and_value(attribute, value)),
            &PatternNonValuePlace::Entid(id) =>
                schema.attribute_for_entid(id)
                      .ok_or_else(|| EmptyBecause::InvalidAttributeEntid(id))
                      .and_then(|attribute| self.table_for_attribute_and_value(attribute, value)),
            // TODO: In a prepared context, defer this decision until a second algebrizing phase.
            // #278.
            &PatternNonValuePlace::Placeholder =>
                self.table_for_unknown_attribute(value),
            &PatternNonValuePlace::Variable(ref v) => {
                // See if we have a binding for the variable.
                match self.bound_value(v) {
                    // TODO: In a prepared context, defer this decision until a second algebrizing phase.
                    // #278.
                    None =>
                        self.table_for_unknown_attribute(value),
                    Some(TypedValue::Ref(id)) =>
                        // Recurse: it's easy.
                        self.table_for_places(schema, &PatternNonValuePlace::Entid(id), value),
                    Some(TypedValue::Keyword(ref kw)) =>
                        // Don't recurse: avoid needing to clone the keyword.
                        schema.attribute_for_ident(kw)
                              .ok_or_else(|| EmptyBecause::InvalidAttributeIdent(kw.cloned()))
                              .and_then(|attribute| self.table_for_attribute_and_value(attribute, value)),
                    Some(v) => {
                        // This pattern cannot match: the caller has bound a non-entity value to an
                        // attribute place.
                        Err(EmptyBecause::InvalidBinding(DatomsColumn::Attribute, v.clone()))
                    },
                }
            },
        }
    }

    /// Produce a (table, alias) pair to handle the provided pattern.
    /// This is a mutating method because it mutates the aliaser function!
    /// Note that if this function decides that a pattern cannot match, it will flip
    /// `is_known_empty`.
    fn alias_table<'s, 'a>(&mut self, schema: &'s Schema, pattern: &'a Pattern) -> Option<SourceAlias> {
        self.table_for_places(schema, &pattern.attribute, &pattern.value)
            .map_err(|reason| {
                self.mark_known_empty(reason);
            })
            .map(|table| SourceAlias(table, (self.aliaser)(table)))
            .ok()
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

    /// Take the contents of `column_bindings` and generate inter-constraints for the appropriate
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
    pub fn expand_column_bindings(&mut self) {
        for cols in self.column_bindings.values() {
            if cols.len() > 1 {
                let ref primary = cols[0];
                let secondaries = cols.iter().skip(1);
                for secondary in secondaries {
                    // TODO: if both primary and secondary are .v, should we make sure
                    // the type tag columns also match?
                    // We don't do so in the ClojureScript version.
                    self.wheres.add_intersection(ColumnConstraint::Equals(primary.clone(), QueryValue::Column(secondary.clone())));
                }
            }
        }
    }

    /// Eliminate any type extractions for variables whose types are definitely known.
    pub fn prune_extracted_types(&mut self) {
        if self.extracted_types.is_empty() || self.known_types.is_empty() {
            return;
        }
        for (var, types) in self.known_types.iter() {
            if types.len() == 1 {
                self.extracted_types.remove(var);
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

impl ConjoiningClauses {
    // This is here, rather than in `lib.rs`, because it's recursive: `or` can contain `or`,
    // and so on.
    pub fn apply_clause(&mut self, schema: &Schema, where_clause: WhereClause) -> Result<()> {
        match where_clause {
            WhereClause::Pattern(p) => {
                self.apply_pattern(schema, p);
                Ok(())
            },
            WhereClause::Pred(p) => {
                self.apply_predicate(schema, p)
            },
            WhereClause::OrJoin(o) => {
                validate_or_join(&o)?;
                self.apply_or_join(schema, o)
            },
            _ => unimplemented!(),
        }
    }
}

// These are helpers that tests use to build Schema instances.
#[cfg(test)]
fn associate_ident(schema: &mut Schema, i: NamespacedKeyword, e: Entid) {
    schema.entid_map.insert(e, i.clone());
    schema.ident_map.insert(i.clone(), e);
}

#[cfg(test)]
fn add_attribute(schema: &mut Schema, e: Entid, a: Attribute) {
    schema.schema_map.insert(e, a);
}

#[cfg(test)]
pub fn ident(ns: &str, name: &str) -> PatternNonValuePlace {
    PatternNonValuePlace::Ident(::std::rc::Rc::new(NamespacedKeyword::new(ns, name)))
}