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

/// Literal `Value` instances in the the "db" namespace.
///
/// Used through-out the transactor to match core DB constructs.

use edn::types::Value;
use edn::symbols;

/// Declare a lazy static `ident` of type `Value::NamespacedKeyword` with the given `namespace` and
/// `name`.
///
/// It may look surprising that we declare a new `lazy_static!` block rather than including
/// invocations inside an existing `lazy_static!` block.  The latter cannot be done, since macros
/// are expanded outside-in.  Looking at the `lazy_static!` source suggests that there is no harm in
/// repeating that macro, since internally a multi-`static` block is expanded into many
/// single-`static` blocks.
///
/// TODO: take just ":db.part/db" and define DB_PART_DB using "db.part" and "db".
macro_rules! lazy_static_namespaced_keyword_value (
    ($tag:ident, $namespace:expr, $name:expr) => (
        lazy_static! {
            pub static ref $tag: Value = {
                Value::NamespacedKeyword(symbols::NamespacedKeyword::namespaced($namespace, $name))
            };
        }
    )
);

lazy_static_namespaced_keyword_value!(DB_ADD, "db", "add");
lazy_static_namespaced_keyword_value!(DB_ALTER_ATTRIBUTE, "db.alter", "attribute");
lazy_static_namespaced_keyword_value!(DB_CARDINALITY, "db", "cardinality");
lazy_static_namespaced_keyword_value!(DB_CARDINALITY_MANY, "db.cardinality", "many");
lazy_static_namespaced_keyword_value!(DB_CARDINALITY_ONE, "db.cardinality", "one");
lazy_static_namespaced_keyword_value!(DB_FULLTEXT, "db", "fulltext");
lazy_static_namespaced_keyword_value!(DB_IDENT, "db", "ident");
lazy_static_namespaced_keyword_value!(DB_INDEX, "db", "index");
lazy_static_namespaced_keyword_value!(DB_INSTALL_ATTRIBUTE, "db.install", "attribute");
lazy_static_namespaced_keyword_value!(DB_IS_COMPONENT, "db", "isComponent");
lazy_static_namespaced_keyword_value!(DB_NO_HISTORY, "db", "noHistory");
lazy_static_namespaced_keyword_value!(DB_PART_DB, "db.part", "db");
lazy_static_namespaced_keyword_value!(DB_RETRACT, "db", "retract");
lazy_static_namespaced_keyword_value!(DB_TYPE_BOOLEAN, "db.type", "boolean");
lazy_static_namespaced_keyword_value!(DB_TYPE_DOUBLE, "db.type", "double");
lazy_static_namespaced_keyword_value!(DB_TYPE_INSTANT, "db.type", "instant");
lazy_static_namespaced_keyword_value!(DB_TYPE_KEYWORD, "db.type", "keyword");
lazy_static_namespaced_keyword_value!(DB_TYPE_LONG, "db.type", "long");
lazy_static_namespaced_keyword_value!(DB_TYPE_REF, "db.type", "ref");
lazy_static_namespaced_keyword_value!(DB_TYPE_STRING, "db.type", "string");
lazy_static_namespaced_keyword_value!(DB_TYPE_URI, "db.type", "uri");
lazy_static_namespaced_keyword_value!(DB_TYPE_UUID, "db.type", "uuid");
lazy_static_namespaced_keyword_value!(DB_UNIQUE, "db", "unique");
lazy_static_namespaced_keyword_value!(DB_UNIQUE_IDENTITY, "db.unique", "identity");
lazy_static_namespaced_keyword_value!(DB_UNIQUE_VALUE, "db.unique", "value");
lazy_static_namespaced_keyword_value!(DB_VALUE_TYPE, "db", "valueType");
