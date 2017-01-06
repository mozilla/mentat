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

use clap::{App, Arg, SubCommand};

fn main() {
    let matches = App::new("Datomish")
    .arg_from_usage("--license 'display the license file'")
                        .subcommand(SubCommand::with_name("serve")
                                                .about("Starts a server")
                                                .arg(Arg::with_name("debug")
                                                    .long("debug")
                                                    .help("Print debugging info"))
                                                .arg(Arg::with_name("database")
                                                     .short("d")
                                                     .long("database")
                                                     .value_name("FILE")
                                                     .help("Path to the Datomish database to serve")
                                                     .default_value("temp.db")
                                                     .takes_value(true))
                                                .arg(Arg::with_name("port")
                                                     .short("p")
                                                     .long("port")
                                                     .value_name("INTEGER")
                                                     .help("Port to serve from, i.e. `localhost:PORT`")
                                                     .default_value("3333")
                                                     .takes_value(true)))
                        .get_matches();

    // You can get the independent subcommand matches (which function exactly like App matches)
    if let Some(ref matches) = matches.subcommand_matches("serve") {
        // Safe to use unwrap() because of the required() option
        let debug = matches.is_present("debug");
        println!("This doesn't work yet, but it will eventually serve the following database: {} on port: {}.  Debugging={}",
                    matches.value_of("database").unwrap(),
                    matches.value_of("port").unwrap(),
                    debug);
    } else {
        println!("Sorry, the cli currently only supports the serve subcommand.  Try `cargo run serve -- --help`");
    }
}
