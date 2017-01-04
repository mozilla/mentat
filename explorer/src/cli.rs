// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate clap;

use std::str::FromStr;

pub struct Args {
    pub database: String,
    pub port: u16,
}

pub fn get_args() -> Args {
    let matches = clap::App::new("Datomish Explorer")
        .version("1.0")
        .author("Mozilla")
        .about("An in-browser IDE for exploring a Datomish database")
        .arg(clap::Arg::with_name("database")
             .short("d")
             .long("database")
             .value_name("FILE")
             .help("Path to the Datomish database to explore")
             .takes_value(true)
             .required(true))
        .arg(clap::Arg::with_name("port")
             .short("p")
             .long("port")
             .value_name("INTEGER")
             .help("Port to serve explorer from, i.e. `localhost:PORT`")
             .default_value("3333")
             .takes_value(true))
        .get_matches();

    Args {
        port: u16::from_str(matches.value_of("port").unwrap()).unwrap(),
        database: matches.value_of("database").unwrap().to_string(),
    }
}
