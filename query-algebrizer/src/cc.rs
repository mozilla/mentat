// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate mentat_core;
extern crate mentat_query;

use std::fmt::{
    Debug,
    Formatter,
};

use std::collections::{
    BTreeMap,
    BTreeSet,
    HashSet,
};

use std::collections::btree_map::Entry;

use self::mentat_core::{
    Attribute,
    Entid,
    Schema,
    TypedValue,
    ValueType,
};

use self::mentat_query::{
    FnArg,
    NamespacedKeyword,
    NonIntegerConstant,
    Pattern,
    PatternNonValuePlace,
    PatternValuePlace,
    PlainSymbol,
    Predicate,
    SrcVar,
    Variable,
};

use errors::{
    Error,
    ErrorKind,
    Result,
};

use types::{
    ColumnConstraint,
    DatomsColumn,
    DatomsTable,
    NumericComparison,
    QualifiedAlias,
    QueryValue,
    SourceAlias,
    TableAlias,
};

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

trait OptionEffect<T> {
    fn when_not<F: FnOnce()>(self, f: F) -> Option<T>;
}

impl<T> OptionEffect<T> for Option<T> {
    fn when_not<F: FnOnce()>(self, f: F) -> Option<T> {
        if self.is_none() {
            f();
        }
        self
    }
}

fn unit_type_set(t: ValueType) -> HashSet<ValueType> {
    let mut s = HashSet::with_capacity(1);
    s.insert(t);
    s
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
    pub wheres: Vec<ColumnConstraint>,

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

#[derive(PartialEq)]
pub enum EmptyBecause {
    // Var, existing, desired.
    TypeMismatch(Variable, HashSet<ValueType>, ValueType),
    NonNumericArgument,
    UnresolvedIdent(NamespacedKeyword),
    InvalidAttributeIdent(NamespacedKeyword),
    InvalidAttributeEntid(Entid),
    InvalidBinding(DatomsColumn, TypedValue),
    ValueTypeMismatch(ValueType, TypedValue),
    AttributeLookupFailed,         // Catch-all, because the table lookup code is lazy. TODO
}

impl Debug for EmptyBecause {
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        use self::EmptyBecause::*;
        match self {
            &TypeMismatch(ref var, ref existing, ref desired) => {
                write!(f, "Type mismatch: {:?} can't be {:?}, because it's already {:?}",
                       var, desired, existing)
            },
            &NonNumericArgument => {
                write!(f, "Non-numeric argument in numeric place")
            },
            &UnresolvedIdent(ref kw) => {
                write!(f, "Couldn't resolve keyword {}", kw)
            },
            &InvalidAttributeIdent(ref kw) => {
                write!(f, "{} does not name an attribute", kw)
            },
            &InvalidAttributeEntid(entid) => {
                write!(f, "{} is not an attribute", entid)
            },
            &InvalidBinding(ref column, ref tv) => {
                write!(f, "{:?} cannot name column {:?}", tv, column)
            },
            &ValueTypeMismatch(value_type, ref typed_value) => {
                write!(f, "Type mismatch: {:?} doesn't match attribute type {:?}",
                       typed_value, value_type)
            },
            &AttributeLookupFailed => {
                write!(f, "Attribute lookup failed")
            },
        }
    }
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
            wheres: vec![],
            input_variables: BTreeSet::new(),
            column_bindings: BTreeMap::new(),
            value_bindings: BTreeMap::new(),
            known_types: BTreeMap::new(),
            extracted_types: BTreeMap::new(),
        }
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
          .extend(cc.value_bindings.iter()
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
                            self.mark_known_empty(EmptyBecause::UnresolvedIdent(kw.clone()));
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
        self.wheres.push(ColumnConstraint::Equals(QualifiedAlias(table, column), QueryValue::TypedValue(constant)))
    }

    pub fn constrain_column_to_entity(&mut self, table: TableAlias, column: DatomsColumn, entity: Entid) {
        self.wheres.push(ColumnConstraint::Equals(QualifiedAlias(table, column), QueryValue::Entid(entity)))
    }

    pub fn constrain_attribute(&mut self, table: TableAlias, attribute: Entid) {
        self.constrain_column_to_entity(table, DatomsColumn::Attribute, attribute)
    }

    pub fn constrain_value_to_numeric(&mut self, table: TableAlias, value: i64) {
        self.wheres.push(ColumnConstraint::Equals(
            QualifiedAlias(table, DatomsColumn::Value),
            QueryValue::PrimitiveLong(value)))
    }

    /// Mark the given value as one of the set of numeric types.
    fn constrain_var_to_numeric(&mut self, variable: Variable) {
        let mut numeric_types = HashSet::with_capacity(2);
        numeric_types.insert(ValueType::Double);
        numeric_types.insert(ValueType::Long);

        let entry = self.known_types.entry(variable);
        match entry {
            Entry::Vacant(vacant) => {
                vacant.insert(numeric_types);
            },
            Entry::Occupied(mut occupied) => {
                let narrowed: HashSet<ValueType> = numeric_types.intersection(occupied.get()).cloned().collect();
                match narrowed.len() {
                    0 => {
                        // TODO: can't borrow as mutable more than once!
                        //self.mark_known_empty(EmptyBecause::TypeMismatch(occupied.key().clone(), occupied.get().clone(), ValueType::Double));   // I know…
                    },
                    1 => {
                        // Hooray!
                        self.extracted_types.remove(occupied.key());
                    },
                    _ => {
                    },
                };
                occupied.insert(narrowed);
            },
        }
    }

    /// Constrains the var if there's no existing type.
    /// Marks as known-empty if it's impossible for this type to apply because there's a conflicting
    /// type already known.
    fn constrain_var_to_type(&mut self, variable: Variable, this_type: ValueType) {
        // If this variable now has a known attribute, we can unhook extracted types for
        // any other instances of that variable.
        // For example, given
        //
        // ```edn
        // [:find ?v :where [?x ?a ?v] [?y :foo/int ?v]]
        // ```
        //
        // we will initially choose to extract the type tag for `?v`, but on encountering
        // the second pattern we can avoid that.
        self.extracted_types.remove(&variable);

        // Is there an existing mapping for this variable?
        // Any known inputs have already been added to known_types, and so if they conflict we'll
        // spot it here.
        if let Some(existing) = self.known_types.get(&variable).cloned() {
            // If so, the types must match.
            if !existing.contains(&this_type) {
                self.mark_known_empty(EmptyBecause::TypeMismatch(variable, existing, this_type));
            } else {
                if existing.len() > 1 {
                    // Narrow.
                    self.known_types.insert(variable, unit_type_set(this_type));
                }
            }
        } else {
            // If not, record the one we just determined.
            self.known_types.insert(variable, unit_type_set(this_type));
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

    fn table_for_unknown_attribute<'s, 'a>(&self, value: &'a PatternValuePlace) -> Option<DatomsTable> {
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
    /// attribute that is congruent with the supplied value, we mark the CC as known-empty and
    /// return `None`.
    fn table_for_places<'s, 'a>(&mut self, schema: &'s Schema, attribute: &'a PatternNonValuePlace, value: &'a PatternValuePlace) -> Option<DatomsTable> {
        match attribute {
            &PatternNonValuePlace::Ident(ref kw) =>
                schema.attribute_for_ident(kw)
                      .when_not(|| self.mark_known_empty(EmptyBecause::InvalidAttributeIdent(kw.clone())))
                      .and_then(|attribute| self.table_for_attribute_and_value(attribute, value)),
            &PatternNonValuePlace::Entid(id) =>
                schema.attribute_for_entid(id)
                      .when_not(|| self.mark_known_empty(EmptyBecause::InvalidAttributeEntid(id)))
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
                              .when_not(|| self.mark_known_empty(EmptyBecause::InvalidAttributeIdent(kw.clone())))
                              .and_then(|attribute| self.table_for_attribute_and_value(attribute, value)),
                    Some(v) => {
                        // This pattern cannot match: the caller has bound a non-entity value to an
                        // attribute place. Return `None` and invalidate this CC.
                        self.mark_known_empty(EmptyBecause::InvalidBinding(DatomsColumn::Attribute,
                                                                           v.clone()));
                        None
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
            .when_not(|| assert!(self.is_known_empty))   // table_for_places should have flipped this.
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
                    self.wheres.push(ColumnConstraint::Equals(primary.clone(), QueryValue::Column(secondary.clone())));
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

/// Argument resolution.
impl ConjoiningClauses {
    /// Take a function argument and turn it into a `QueryValue` suitable for use in a concrete
    /// constraint.
    /// Additionally, do two things:
    /// - Mark the pattern as known-empty if any argument is known non-numeric.
    /// - Mark any variables encountered as numeric.
    fn resolve_numeric_argument(&mut self, function: &PlainSymbol, position: usize, arg: FnArg) -> Result<QueryValue> {
        use self::FnArg::*;
        match arg {
            FnArg::Variable(var) => {
                self.constrain_var_to_numeric(var.clone());
                self.column_bindings
                    .get(&var)
                    .and_then(|cols| cols.first().map(|col| QueryValue::Column(col.clone())))
                    .ok_or_else(|| Error::from_kind(ErrorKind::UnboundVariable(var)))
            },
            // Can't be an entid.
            EntidOrInteger(i) => Ok(QueryValue::TypedValue(TypedValue::Long(i))),
            Ident(_) |
            SrcVar(_) |
            Constant(NonIntegerConstant::Boolean(_)) |
            Constant(NonIntegerConstant::Text(_)) |
            Constant(NonIntegerConstant::BigInteger(_)) => {
                self.mark_known_empty(EmptyBecause::NonNumericArgument);
                bail!(ErrorKind::NonNumericArgument(function.clone(), position));
            },
            Constant(NonIntegerConstant::Float(f)) => Ok(QueryValue::TypedValue(TypedValue::Double(f))),
        }
    }


    /// Take a function argument and turn it into a `QueryValue` suitable for use in a concrete
    /// constraint.
    #[allow(dead_code)]
    fn resolve_argument(&self, arg: FnArg) -> Result<QueryValue> {
        use self::FnArg::*;
        match arg {
            FnArg::Variable(var) => {
                self.column_bindings
                    .get(&var)
                    .and_then(|cols| cols.first().map(|col| QueryValue::Column(col.clone())))
                    .ok_or_else(|| Error::from_kind(ErrorKind::UnboundVariable(var)))
            },
            EntidOrInteger(i) => Ok(QueryValue::PrimitiveLong(i)),
            Ident(_) => unimplemented!(),     // TODO
            Constant(NonIntegerConstant::Boolean(val)) => Ok(QueryValue::TypedValue(TypedValue::Boolean(val))),
            Constant(NonIntegerConstant::Float(f)) => Ok(QueryValue::TypedValue(TypedValue::Double(f))),
            Constant(NonIntegerConstant::Text(s)) => Ok(QueryValue::TypedValue(TypedValue::String(s.clone()))),
            Constant(NonIntegerConstant::BigInteger(_)) => unimplemented!(),
            SrcVar(_) => unimplemented!(),
        }
    }
}

/// Application of patterns.
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
                // Placeholders don't contribute any column bindings, nor do
                // they constrain the query -- there's no need to produce
                // IS NOT NULL, because we don't store nulls in our schema.
                (),
            PatternNonValuePlace::Variable(ref v) =>
                self.bind_column_to_var(schema, col.clone(), DatomsColumn::Entity, v.clone()),
            PatternNonValuePlace::Entid(entid) =>
                self.constrain_column_to_entity(col.clone(), DatomsColumn::Entity, entid),
            PatternNonValuePlace::Ident(ref ident) => {
                if let Some(entid) = self.entid_for_ident(schema, ident) {
                    self.constrain_column_to_entity(col.clone(), DatomsColumn::Entity, entid)
                } else {
                    // A resolution failure means we're done here.
                    self.mark_known_empty(EmptyBecause::UnresolvedIdent(ident.clone()));
                    return;
                }
            }
        }

        match pattern.attribute {
            PatternNonValuePlace::Placeholder =>
                (),
            PatternNonValuePlace::Variable(ref v) =>
                self.bind_column_to_var(schema, col.clone(), DatomsColumn::Attribute, v.clone()),
            PatternNonValuePlace::Entid(entid) => {
                if !schema.is_attribute(entid) {
                    // Furthermore, that entid must resolve to an attribute. If it doesn't, this
                    // query is meaningless.
                    self.mark_known_empty(EmptyBecause::InvalidAttributeEntid(entid));
                    return;
                }
                self.constrain_attribute(col.clone(), entid)
            },
            PatternNonValuePlace::Ident(ref ident) => {
                if let Some(entid) = self.entid_for_ident(schema, ident) {
                    self.constrain_attribute(col.clone(), entid);

                    if !schema.is_attribute(entid) {
                        self.mark_known_empty(EmptyBecause::InvalidAttributeIdent(ident.clone()));
                        return;
                    }
                } else {
                    // A resolution failure means we're done here.
                    self.mark_known_empty(EmptyBecause::UnresolvedIdent(ident.clone()));
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
                    self.constrain_var_to_type(v.clone(), this_type);
                    if self.is_known_empty {
                        return;
                    }
                }

                self.bind_column_to_var(schema, col.clone(), DatomsColumn::Value, v.clone());
            },
            PatternValuePlace::EntidOrInteger(i) =>
                // If we know the valueType, then we can determine whether this is an entid or an
                // integer. If we don't, then we must generate a more general query with a
                // value_type_tag.
                if let Some(ValueType::Ref) = value_type {
                    self.constrain_column_to_entity(col.clone(), DatomsColumn::Value, i);
                } else {
                    // If we have a pattern like:
                    //
                    //   `[123 ?a 1]`
                    //
                    // then `1` could be an entid (ref), a long, a boolean, or an instant.
                    //
                    // We represent these constraints during execution:
                    //
                    // - Constraining the value column to the plain numeric value '1'.
                    // - Constraining its type column to one of a set of types.
                    //
                    self.constrain_value_to_numeric(col.clone(), i);
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
                        self.mark_known_empty(EmptyBecause::UnresolvedIdent(kw.clone()));
                        return;
                    }
                } else {
                    // It must be a keyword.
                    self.constrain_column_to_constant(col.clone(), DatomsColumn::Value, TypedValue::Keyword(kw.clone()));
                    self.wheres.push(ColumnConstraint::HasType(col.clone(), ValueType::Keyword));
                };
            },
            PatternValuePlace::Constant(ref c) => {
                // TODO: don't allocate.
                let typed_value = c.clone().into_typed_value();
                if !typed_value.is_congruent_with(value_type) {
                    // If the attribute and its value don't match, the pattern must fail.
                    // We can never have a congruence failure if `value_type` is `None`, so we
                    // forcibly unwrap here.
                    let value_type = value_type.expect("Congruence failure but couldn't unwrap");
                    let why = EmptyBecause::ValueTypeMismatch(value_type, typed_value);
                    self.mark_known_empty(why);
                    return;
                }

                // TODO: if we don't know the type of the attribute because we don't know the
                // attribute, we can actually work backwards to the set of appropriate attributes
                // from the type of the value itself! #292.
                let typed_value_type = typed_value.value_type();
                self.constrain_column_to_constant(col.clone(), DatomsColumn::Value, typed_value);

                // If we can't already determine the range of values in the DB from the attribute,
                // then we must also constrain the type tag.
                //
                // Input values might be:
                //
                // - A long. This is handled by EntidOrInteger.
                // - A boolean. This is unambiguous.
                // - A double. This is currently unambiguous, though note that SQLite will equate 5.0 with 5.
                // - A string. This is unambiguous.
                // - A keyword. This is unambiguous.
                //
                // Because everything we handle here is unambiguous, we generate a single type
                // restriction from the value type of the typed value.
                if value_type.is_none() {
                    self.wheres.push(ColumnConstraint::HasType(col.clone(), typed_value_type));
                }

            },
        }

    }

    pub fn apply_pattern<'s, 'p>(&mut self, schema: &'s Schema, pattern: Pattern) {
        // For now we only support the default source.
        match pattern.source {
            Some(SrcVar::DefaultSrc) | None => (),
            _ => unimplemented!(),
        };

        if let Some(alias) = self.alias_table(schema, &pattern) {
            self.apply_pattern_clause_for_alias(schema, &pattern, &alias);
            self.from.push(alias);
        } else {
            // We didn't determine a table, likely because there was a mismatch
            // between an attribute and a value.
            // We know we cannot return a result, so we short-circuit here.
            self.mark_known_empty(EmptyBecause::AttributeLookupFailed);
            return;
        }
    }
}

/// Application of predicates.
impl ConjoiningClauses {
    /// There are several kinds of predicates/functions in our Datalog:
    /// - A limited set of binary comparison operators: < > <= >= !=.
    ///   These are converted into SQLite binary comparisons and some type constraints.
    /// - A set of predicates like `fulltext` and `get-else` that are translated into
    ///   SQL `MATCH`es or joins, yielding bindings.
    /// - In the future, some predicates that are implemented via function calls in SQLite.
    ///
    /// At present we have implemented only the five built-in comparison binary operators.
    pub fn apply_predicate<'s, 'p>(&mut self, schema: &'s Schema, predicate: Predicate) -> Result<()> {
        // Because we'll be growing the set of built-in predicates, handling each differently,
        // and ultimately allowing user-specified predicates, we match on the predicate name first.
        if let Some(op) = NumericComparison::from_datalog_operator(predicate.operator.0.as_str()) {
            self.apply_numeric_predicate(schema, op, predicate)
        } else {
            bail!(ErrorKind::UnknownFunction(predicate.operator.clone()))
        }
    }

    /// This function:
    /// - Resolves variables and converts types to those more amenable to SQL.
    /// - Ensures that the predicate functions name a known operator.
    /// - Accumulates a `NumericInequality` constraint into the `wheres` list.
    #[allow(unused_variables)]
    pub fn apply_numeric_predicate<'s, 'p>(&mut self, schema: &'s Schema, comparison: NumericComparison, predicate: Predicate) -> Result<()> {
        if predicate.args.len() != 2 {
            bail!(ErrorKind::InvalidNumberOfArguments(predicate.operator.clone(), predicate.args.len(), 2));
        }

        // Go from arguments -- parser output -- to columns or values.
        // Any variables that aren't bound by this point in the linear processing of clauses will
        // cause the application of the predicate to fail.
        let mut args = predicate.args.into_iter();
        let left = self.resolve_numeric_argument(&predicate.operator, 0, args.next().unwrap())?;
        let right = self.resolve_numeric_argument(&predicate.operator, 1, args.next().unwrap())?;

        // These arguments must be variables or numeric constants.
        // TODO: generalize argument resolution and validation for different kinds of predicates:
        // as we process `(< ?x 5)` we are able to check or deduce that `?x` is numeric, and either
        // simplify the pattern or optimize the rest of the query.
        // To do so needs a slightly more sophisticated representation of type constraints — a set,
        // not a single `Option`.

        // TODO: static evaluation. #383.
        let constraint = ColumnConstraint::NumericInequality {
            operator: comparison,
            left: left,
            right: right,
        };
        self.wheres.push(constraint);
        Ok(())
    }
}

#[cfg(test)]
mod testing {
    use super::*;
    use mentat_core::attribute::Unique;
    use mentat_query::PlainSymbol;

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

        cc.apply_pattern(&schema, Pattern {
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

        cc.apply_pattern(&schema, Pattern {
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
        cc.apply_pattern(&schema, Pattern {
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
        assert_eq!(cc.known_type(&x).unwrap(), ValueType::Ref);

        // ?x is bound to datoms0.e.
        assert_eq!(cc.column_bindings.get(&x).unwrap(), &vec![d0_e.clone()]);

        // Our 'where' clauses are two:
        // - datoms0.a = 99
        // - datoms0.v = true
        // No need for a type tag constraint, because the attribute is known.
        assert_eq!(cc.wheres, vec![
                   ColumnConstraint::Equals(d0_a, QueryValue::Entid(99)),
                   ColumnConstraint::Equals(d0_v, QueryValue::TypedValue(TypedValue::Boolean(true))),
        ]);
    }

    #[test]
    fn test_apply_unattributed_pattern() {
        let mut cc = ConjoiningClauses::default();
        let schema = Schema::default();

        let x = Variable(PlainSymbol::new("?x"));
        cc.apply_pattern(&schema, Pattern {
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
        assert_eq!(cc.known_type(&x).unwrap(), ValueType::Ref);

        // ?x is bound to datoms0.e.
        assert_eq!(cc.column_bindings.get(&x).unwrap(), &vec![d0_e.clone()]);

        // Our 'where' clauses are two:
        // - datoms0.v = true
        // - datoms0.value_type_tag = boolean
        // TODO: implement expand_type_tags.
        assert_eq!(cc.wheres, vec![
                   ColumnConstraint::Equals(d0_v, QueryValue::TypedValue(TypedValue::Boolean(true))),
                   ColumnConstraint::HasType("datoms00".to_string(), ValueType::Boolean),
        ]);
    }

    /// This test ensures that we do less work if we know the attribute thanks to a var lookup.
    #[test]
    fn test_apply_unattributed_but_bound_pattern_with_returned() {
        let mut cc = ConjoiningClauses::default();
        let mut schema = Schema::default();
        associate_ident(&mut schema, NamespacedKeyword::new("foo", "bar"), 99);
        add_attribute(&mut schema, 99, Attribute {
            value_type: ValueType::Boolean,
            ..Default::default()
        });

        let x = Variable(PlainSymbol::new("?x"));
        let a = Variable(PlainSymbol::new("?a"));
        let v = Variable(PlainSymbol::new("?v"));

        cc.input_variables.insert(a.clone());
        cc.value_bindings.insert(a.clone(), TypedValue::Keyword(NamespacedKeyword::new("foo", "bar")));
        cc.apply_pattern(&schema, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            attribute: PatternNonValuePlace::Variable(a.clone()),
            value: PatternValuePlace::Variable(v.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });

        // println!("{:#?}", cc);

        let d0_e = QualifiedAlias("datoms00".to_string(), DatomsColumn::Entity);
        let d0_a = QualifiedAlias("datoms00".to_string(), DatomsColumn::Attribute);

        assert!(!cc.is_known_empty);
        assert_eq!(cc.from, vec![SourceAlias(DatomsTable::Datoms, "datoms00".to_string())]);

        // ?x must be a ref.
        assert_eq!(cc.known_type(&x).unwrap(), ValueType::Ref);

        // ?x is bound to datoms0.e.
        assert_eq!(cc.column_bindings.get(&x).unwrap(), &vec![d0_e.clone()]);
        assert_eq!(cc.wheres, vec![
                   ColumnConstraint::Equals(d0_a, QueryValue::Entid(99)),
        ]);
    }

    /// Queries that bind non-entity values to entity places can't return results.
    #[test]
    fn test_bind_the_wrong_thing() {
        let mut cc = ConjoiningClauses::default();
        let schema = Schema::default();

        let x = Variable(PlainSymbol::new("?x"));
        let a = Variable(PlainSymbol::new("?a"));
        let v = Variable(PlainSymbol::new("?v"));
        let hello = TypedValue::String("hello".to_string());

        cc.input_variables.insert(a.clone());
        cc.value_bindings.insert(a.clone(), hello.clone());
        cc.apply_pattern(&schema, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            attribute: PatternNonValuePlace::Variable(a.clone()),
            value: PatternValuePlace::Variable(v.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });

        assert!(cc.is_known_empty);
        assert_eq!(cc.empty_because.unwrap(), EmptyBecause::InvalidBinding(DatomsColumn::Attribute, hello));
    }


    /// This test ensures that we query all_datoms if we're possibly retrieving a string.
    #[test]
    fn test_apply_unattributed_pattern_with_returned() {
        let mut cc = ConjoiningClauses::default();
        let schema = Schema::default();

        let x = Variable(PlainSymbol::new("?x"));
        let a = Variable(PlainSymbol::new("?a"));
        let v = Variable(PlainSymbol::new("?v"));
        cc.apply_pattern(&schema, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            attribute: PatternNonValuePlace::Variable(a.clone()),
            value: PatternValuePlace::Variable(v.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });

        // println!("{:#?}", cc);

        let d0_e = QualifiedAlias("all_datoms00".to_string(), DatomsColumn::Entity);

        assert!(!cc.is_known_empty);
        assert_eq!(cc.from, vec![SourceAlias(DatomsTable::AllDatoms, "all_datoms00".to_string())]);

        // ?x must be a ref.
        assert_eq!(cc.known_type(&x).unwrap(), ValueType::Ref);

        // ?x is bound to datoms0.e.
        assert_eq!(cc.column_bindings.get(&x).unwrap(), &vec![d0_e.clone()]);
        assert_eq!(cc.wheres, vec![]);
    }

    /// This test ensures that we query all_datoms if we're looking for a string.
    #[test]
    fn test_apply_unattributed_pattern_with_string_value() {
        let mut cc = ConjoiningClauses::default();
        let schema = Schema::default();

        let x = Variable(PlainSymbol::new("?x"));
        cc.apply_pattern(&schema, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            attribute: PatternNonValuePlace::Placeholder,
            value: PatternValuePlace::Constant(NonIntegerConstant::Text("hello".to_string())),
            tx: PatternNonValuePlace::Placeholder,
        });

        // println!("{:#?}", cc);

        let d0_e = QualifiedAlias("all_datoms00".to_string(), DatomsColumn::Entity);
        let d0_v = QualifiedAlias("all_datoms00".to_string(), DatomsColumn::Value);

        assert!(!cc.is_known_empty);
        assert_eq!(cc.from, vec![SourceAlias(DatomsTable::AllDatoms, "all_datoms00".to_string())]);

        // ?x must be a ref.
        assert_eq!(cc.known_type(&x).unwrap(), ValueType::Ref);

        // ?x is bound to datoms0.e.
        assert_eq!(cc.column_bindings.get(&x).unwrap(), &vec![d0_e.clone()]);

        // Our 'where' clauses are two:
        // - datoms0.v = 'hello'
        // - datoms0.value_type_tag = string
        // TODO: implement expand_type_tags.
        assert_eq!(cc.wheres, vec![
                   ColumnConstraint::Equals(d0_v, QueryValue::TypedValue(TypedValue::String("hello".to_string()))),
                   ColumnConstraint::HasType("all_datoms00".to_string(), ValueType::String),
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
            ..Default::default()
        });

        let x = Variable(PlainSymbol::new("?x"));
        let y = Variable(PlainSymbol::new("?y"));
        cc.apply_pattern(&schema, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            attribute: PatternNonValuePlace::Ident(NamespacedKeyword::new("foo", "roz")),
            value: PatternValuePlace::Constant(NonIntegerConstant::Text("idgoeshere".to_string())),
            tx: PatternNonValuePlace::Placeholder,
        });
        cc.apply_pattern(&schema, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            attribute: PatternNonValuePlace::Ident(NamespacedKeyword::new("foo", "bar")),
            value: PatternValuePlace::Variable(y.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });

        // Finally, expand column bindings to get the overlaps for ?x.
        cc.expand_column_bindings();

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
        assert_eq!(cc.known_type(&x).unwrap(), ValueType::Ref);

        // ?x is bound to datoms0.e and datoms1.e.
        assert_eq!(cc.column_bindings.get(&x).unwrap(),
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
                   ColumnConstraint::Equals(d0_a, QueryValue::Entid(98)),
                   ColumnConstraint::Equals(d0_v, QueryValue::TypedValue(TypedValue::String("idgoeshere".to_string()))),
                   ColumnConstraint::Equals(d1_a, QueryValue::Entid(99)),
                   ColumnConstraint::Equals(d0_e, QueryValue::Column(d1_e)),
        ]);
    }

    #[test]
    fn test_value_bindings() {
        let mut schema = Schema::default();

        associate_ident(&mut schema, NamespacedKeyword::new("foo", "bar"), 99);
        add_attribute(&mut schema, 99, Attribute {
            value_type: ValueType::Boolean,
            ..Default::default()
        });

        let x = Variable(PlainSymbol::new("?x"));
        let y = Variable(PlainSymbol::new("?y"));

        let b: BTreeMap<Variable, TypedValue> =
            vec![(y.clone(), TypedValue::Boolean(true))].into_iter().collect();
        let mut cc = ConjoiningClauses::with_value_bindings(b);

        cc.apply_pattern(&schema, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            attribute: PatternNonValuePlace::Ident(NamespacedKeyword::new("foo", "bar")),
            value: PatternValuePlace::Variable(y.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });

        let d0_e = QualifiedAlias("datoms00".to_string(), DatomsColumn::Entity);
        let d0_a = QualifiedAlias("datoms00".to_string(), DatomsColumn::Attribute);
        let d0_v = QualifiedAlias("datoms00".to_string(), DatomsColumn::Value);

        // ?y has been expanded into `true`.
        assert_eq!(cc.wheres, vec![
                   ColumnConstraint::Equals(d0_a, QueryValue::Entid(99)),
                   ColumnConstraint::Equals(d0_v, QueryValue::TypedValue(TypedValue::Boolean(true))),
        ]);

        // There is no binding for ?y.
        assert!(!cc.column_bindings.contains_key(&y));

        // ?x is bound to the entity.
        assert_eq!(cc.column_bindings.get(&x).unwrap(),
                   &vec![d0_e.clone()]);
    }

    #[test]
    /// Bind a value to a variable in a query where the type of the value disagrees with the type of
    /// the variable inferred from known attributes.
    fn test_value_bindings_type_disagreement() {
        let mut schema = Schema::default();

        associate_ident(&mut schema, NamespacedKeyword::new("foo", "bar"), 99);
        add_attribute(&mut schema, 99, Attribute {
            value_type: ValueType::Boolean,
            ..Default::default()
        });

        let x = Variable(PlainSymbol::new("?x"));
        let y = Variable(PlainSymbol::new("?y"));

        let b: BTreeMap<Variable, TypedValue> =
            vec![(y.clone(), TypedValue::Long(42))].into_iter().collect();
        let mut cc = ConjoiningClauses::with_value_bindings(b);

        cc.apply_pattern(&schema, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            attribute: PatternNonValuePlace::Ident(NamespacedKeyword::new("foo", "bar")),
            value: PatternValuePlace::Variable(y.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });

        // The type of the provided binding doesn't match the type of the attribute.
        assert!(cc.is_known_empty);
    }

    #[test]
    /// Bind a non-textual value to a variable in a query where the variable is used as the value
    /// of a fulltext-valued attribute.
    fn test_fulltext_type_disagreement() {
        let mut schema = Schema::default();

        associate_ident(&mut schema, NamespacedKeyword::new("foo", "bar"), 99);
        add_attribute(&mut schema, 99, Attribute {
            value_type: ValueType::String,
            fulltext: true,
            ..Default::default()
        });

        let x = Variable(PlainSymbol::new("?x"));
        let y = Variable(PlainSymbol::new("?y"));

        let b: BTreeMap<Variable, TypedValue> =
            vec![(y.clone(), TypedValue::Long(42))].into_iter().collect();
        let mut cc = ConjoiningClauses::with_value_bindings(b);

        cc.apply_pattern(&schema, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            attribute: PatternNonValuePlace::Ident(NamespacedKeyword::new("foo", "bar")),
            value: PatternValuePlace::Variable(y.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });

        // The type of the provided binding doesn't match the type of the attribute.
        assert!(cc.is_known_empty);
    }

    #[test]
    /// Apply two patterns: a pattern and a numeric predicate.
    /// Verify that after application of the predicate we know that the value
    /// must be numeric.
    fn test_apply_numeric_predicate() {
        let mut cc = ConjoiningClauses::default();
        let mut schema = Schema::default();

        associate_ident(&mut schema, NamespacedKeyword::new("foo", "bar"), 99);
        add_attribute(&mut schema, 99, Attribute {
            value_type: ValueType::Long,
            ..Default::default()
        });

        let x = Variable(PlainSymbol::new("?x"));
        let y = Variable(PlainSymbol::new("?y"));
        cc.apply_pattern(&schema, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            attribute: PatternNonValuePlace::Placeholder,
            value: PatternValuePlace::Variable(y.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });
        assert!(!cc.is_known_empty);

        let op = PlainSymbol::new("<");
        let comp = NumericComparison::from_datalog_operator(op.plain_name()).unwrap();
        assert!(cc.apply_numeric_predicate(&schema, comp, Predicate {
             operator: op,
             args: vec![
                FnArg::Variable(Variable(PlainSymbol::new("?y"))), FnArg::EntidOrInteger(10),
            ]}).is_ok());

        assert!(!cc.is_known_empty);

        // Finally, expand column bindings to get the overlaps for ?x.
        cc.expand_column_bindings();
        assert!(!cc.is_known_empty);

        // After processing those two clauses, we know that ?y must be numeric, but not exactly
        // which type it must be.
        assert_eq!(None, cc.known_type(&y));      // Not just one.
        let expected: HashSet<ValueType> = vec![ValueType::Double, ValueType::Long].into_iter().collect();
        assert_eq!(Some(&expected), cc.known_types.get(&y));

        let clauses = cc.wheres;
        assert_eq!(clauses.len(), 1);
        assert_eq!(clauses[0], ColumnConstraint::NumericInequality {
            operator: NumericComparison::LessThan,
            left: QueryValue::Column(cc.column_bindings.get(&y).unwrap()[0].clone()),
            right: QueryValue::TypedValue(TypedValue::Long(10)),
        });
    }

    #[test]
    /// Apply three patterns: an unbound pattern to establish a value var,
    /// a predicate to constrain the val to numeric types, and a third pattern to conflict with the
    /// numeric types and cause the pattern to fail.
    fn test_apply_conflict_with_numeric_range() {
        let mut cc = ConjoiningClauses::default();
        let mut schema = Schema::default();

        associate_ident(&mut schema, NamespacedKeyword::new("foo", "bar"), 99);
        associate_ident(&mut schema, NamespacedKeyword::new("foo", "roz"), 98);
        add_attribute(&mut schema, 99, Attribute {
            value_type: ValueType::Long,
            ..Default::default()
        });
        add_attribute(&mut schema, 98, Attribute {
            value_type: ValueType::String,
            unique: Some(Unique::Identity),
            ..Default::default()
        });

        let x = Variable(PlainSymbol::new("?x"));
        let y = Variable(PlainSymbol::new("?y"));
        cc.apply_pattern(&schema, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            attribute: PatternNonValuePlace::Placeholder,
            value: PatternValuePlace::Variable(y.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });
        assert!(!cc.is_known_empty);

        let op = PlainSymbol::new(">=");
        let comp = NumericComparison::from_datalog_operator(op.plain_name()).unwrap();
        assert!(cc.apply_numeric_predicate(&schema, comp, Predicate {
             operator: op,
             args: vec![
                FnArg::Variable(Variable(PlainSymbol::new("?y"))), FnArg::EntidOrInteger(10),
            ]}).is_ok());

        assert!(!cc.is_known_empty);
        cc.apply_pattern(&schema, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            attribute: PatternNonValuePlace::Ident(NamespacedKeyword::new("foo", "roz")),
            value: PatternValuePlace::Variable(y.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });

        // Finally, expand column bindings to get the overlaps for ?x.
        cc.expand_column_bindings();

        assert!(cc.is_known_empty);
        assert_eq!(cc.empty_because.unwrap(),
                   EmptyBecause::TypeMismatch(y.clone(),
                                              vec![ValueType::Double, ValueType::Long].into_iter()
                                                                                      .collect(),
                                              ValueType::String));
    }

    #[test]
    /// Apply two patterns with differently typed attributes, but sharing a variable in the value
    /// place. No value can bind to a variable and match both types, so the CC is known to return
    /// no results.
    fn test_apply_two_conflicting_known_patterns() {
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
            unique: Some(Unique::Identity),
            ..Default::default()
        });

        let x = Variable(PlainSymbol::new("?x"));
        let y = Variable(PlainSymbol::new("?y"));
        cc.apply_pattern(&schema, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            attribute: PatternNonValuePlace::Ident(NamespacedKeyword::new("foo", "roz")),
            value: PatternValuePlace::Variable(y.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });
        cc.apply_pattern(&schema, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            attribute: PatternNonValuePlace::Ident(NamespacedKeyword::new("foo", "bar")),
            value: PatternValuePlace::Variable(y.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });

        // Finally, expand column bindings to get the overlaps for ?x.
        cc.expand_column_bindings();

        assert!(cc.is_known_empty);
        assert_eq!(cc.empty_because.unwrap(),
                   EmptyBecause::TypeMismatch(y.clone(), unit_type_set(ValueType::String), ValueType::Boolean));
    }

    #[test]
    #[should_panic(expected = "assertion failed: cc.is_known_empty")]
    /// This test needs range inference in order to succeed: we must deduce that ?y must
    /// simultaneously be a boolean-valued attribute and a ref-valued attribute, and thus
    /// the CC can never return results.
    fn test_apply_two_implicitly_conflicting_patterns() {
        let mut cc = ConjoiningClauses::default();
        let schema = Schema::default();

        // [:find ?x :where
        //  [?x ?y true]
        //  [?z ?y ?x]]
        let x = Variable(PlainSymbol::new("?x"));
        let y = Variable(PlainSymbol::new("?y"));
        let z = Variable(PlainSymbol::new("?z"));
        cc.apply_pattern(&schema, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            attribute: PatternNonValuePlace::Variable(y.clone()),
            value: PatternValuePlace::Constant(NonIntegerConstant::Boolean(true)),
            tx: PatternNonValuePlace::Placeholder,
        });
        cc.apply_pattern(&schema, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(z.clone()),
            attribute: PatternNonValuePlace::Variable(y.clone()),
            value: PatternValuePlace::Variable(x.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });

        // Finally, expand column bindings to get the overlaps for ?x.
        cc.expand_column_bindings();

        assert!(cc.is_known_empty);
        assert_eq!(cc.empty_because.unwrap(),
                   EmptyBecause::TypeMismatch(x.clone(), unit_type_set(ValueType::Ref), ValueType::Boolean));
    }
}
