// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

// For error_chain:
#![recursion_limit="128"]

#[macro_use]
extern crate error_chain;

#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate serde_derive;

extern crate hyper;
extern crate tokio_core;
extern crate futures;
extern crate serde;
extern crate serde_cbor;
extern crate serde_json;
extern crate mentat_db;
extern crate mentat_core;
extern crate rusqlite;
extern crate uuid;

pub mod schema;
pub mod metadata;
pub mod tx_processor;
pub mod errors;
pub mod syncer;
pub mod tx_mapper;
