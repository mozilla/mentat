// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::env;
extern crate datomish;

// This is just a placeholder to get the project structure in place.
fn main() {
    println!("Loaded {}", datomish::get_name());

    let args: Vec<String> = env::args().collect();
    println!("I got {:?} arguments: {:?}.", args.len() - 1, &args[1..]);
}
