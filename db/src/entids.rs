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

/// Literal `Entid` values in the the "db" namespace.
///
/// Used through-out the transactor to match core DB constructs.

use types::{Entid};

// Added in SQL schema v1.
pub const DB_IDENT: Entid = 1;
pub const DB_PART_DB: Entid = 2;
pub const DB_TX_INSTANT: Entid = 3;
pub const DB_INSTALL_PARTITION: Entid = 4;
pub const DB_INSTALL_VALUE_TYPE: Entid = 5;
pub const DB_INSTALL_ATTRIBUTE: Entid = 6;
pub const DB_VALUE_TYPE: Entid = 7;
pub const DB_CARDINALITY: Entid = 8;
pub const DB_UNIQUE: Entid = 9;
pub const DB_IS_COMPONENT: Entid = 10;
pub const DB_INDEX: Entid = 11;
pub const DB_FULLTEXT: Entid = 12;
pub const DB_NO_HISTORY: Entid = 13;
pub const DB_ADD: Entid = 14;
pub const DB_RETRACT: Entid = 15;
pub const DB_PART_USER: Entid = 16;
pub const DB_PART_TX: Entid = 17;
pub const DB_EXCISE: Entid = 18;
pub const DB_EXCISE_ATTRS: Entid = 19;
pub const DB_EXCISE_BEFORE_T: Entid = 20;
pub const DB_EXCISE_BEFORE: Entid = 21;
pub const DB_ALTER_ATTRIBUTE: Entid = 22;
pub const DB_TYPE_REF: Entid = 23;
pub const DB_TYPE_KEYWORD: Entid = 24;
pub const DB_TYPE_LONG: Entid = 25;
pub const DB_TYPE_DOUBLE: Entid = 26;
pub const DB_TYPE_STRING: Entid = 27;
pub const DB_TYPE_BOOLEAN: Entid = 28;
pub const DB_TYPE_INSTANT: Entid = 29;
pub const DB_TYPE_BYTES: Entid = 30;
pub const DB_CARDINALITY_ONE: Entid = 31;
pub const DB_CARDINALITY_MANY: Entid = 32;
pub const DB_UNIQUE_VALUE: Entid = 33;
pub const DB_UNIQUE_IDENTITY: Entid = 34;
pub const DB_DOC: Entid = 35;

// Added in SQL schema v2.
pub const DB_SCHEMA_VERSION: Entid = 36;
pub const DB_SCHEMA_ATTRIBUTE: Entid = 37;
