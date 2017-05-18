// Copyright 2017 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![crate_name = "mentat_cli"]

#[macro_use] extern crate log;
#[macro_use] extern crate lazy_static;
#[macro_use] extern crate error_chain;

extern crate combine;
extern crate env_logger;
extern crate getopts;
extern crate linefeed;
extern crate rusqlite;

extern crate mentat;
extern crate edn;
extern crate mentat_query;
extern crate mentat_core;
extern crate mentat_db;

use getopts::Options;

pub mod command_parser;
pub mod store;
pub mod input;
pub mod repl;
pub mod errors;

pub fn run() -> i32 {
    env_logger::init().unwrap();

    let args = std::env::args().collect::<Vec<_>>();
    let mut opts = Options::new();

    opts.optopt("d", "", "The path to a database to open", "DATABASE");
    opts.optflag("h", "help", "Print this help message and exit");
    opts.optflag("v", "version", "Print version and exit");

    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(e) => {
            println!("{}: {}", args[0], e);
            return 1;
        }
    };

    if matches.opt_present("version") {
        print_version();
        return 0;
    }

    if matches.opt_present("help") {
        print_usage(&args[0], &opts);
        return 0;
    }

    let db_name = matches.opt_str("d");

    let repl = repl::Repl::new(db_name);
    if repl.is_ok() {
        repl.unwrap().run();
    } else {
        println!("{}", repl.err().unwrap());
    }

    0
}

/// Returns a version string.
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

fn print_usage(arg0: &str, opts: &Options) {
    print!("{}", opts.usage(&format!(
        "Usage: {} [OPTIONS] [FILE]", arg0)));
}

fn print_version() {
    println!("mentat {}", version());
}


#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
    }
}
