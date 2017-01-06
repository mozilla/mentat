// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate iron;

mod cli;

use cli::{get_args};
use iron::prelude::*;
use iron::status;

fn main() {
    let args = get_args();
    fn hello_world(_: &mut Request) -> IronResult<Response> {
        Ok(Response::with((status::Ok, "Hello World!")))
    }

    println!("Running Datomish Explorer at localhost:{} for database at {}", args.port, args.database);
    let _server = Iron::new(hello_world).http(("localhost", args.port)).unwrap();
}
