// Copyright 2017 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::HashMap;
use error_chain::ChainedError;

use command_parser::{
    Command, 
    HELP_COMMAND, 
    OPEN_COMMAND,
    LONG_QUERY_COMMAND,
    SHORT_QUERY_COMMAND,
    LONG_TRANSACT_COMMAND,
    SHORT_TRANSACT_COMMAND,
    READ_COMMAND,
};
use input::InputReader;
use input::InputResult;
use input::InputResult::{
    MetaCommand, 
    Empty, 
    More, 
    Eof
};
use store::{ 
    Store,
    db_output_name
};

lazy_static! {
    static ref COMMAND_HELP: HashMap<&'static str, &'static str> = {
        let mut map = HashMap::new();
        map.insert(HELP_COMMAND, "Show help for commands.");
        map.insert(OPEN_COMMAND, "Open a database at path.");
        map.insert(LONG_QUERY_COMMAND, "Execute a query against the current open database.");
        map.insert(SHORT_QUERY_COMMAND, "Shortcut for `.query`. Execute a query against the current open database.");
        map.insert(LONG_TRANSACT_COMMAND, "Execute a transact against the current open database.");
        map.insert(SHORT_TRANSACT_COMMAND, "Shortcut for `.transact`. Execute a transact against the current open database.");
        map.insert(READ_COMMAND, "Read a file containing one or more complete edn transact statements. Transacts each edn in turn.");
        map
    };
}

/// Executes input and maintains state of persistent items.
pub struct Repl {
    store: Store
}

impl Repl {
    /// Constructs a new `Repl`.
    pub fn new() -> Result<Repl, String> {
        let store = Store::new(None).map_err(|e| e.to_string())?;
        Ok(Repl{
            store: store,
        })
    }

    /// Runs the REPL interactively.
    pub fn run(&mut self, startup_commands: Option<Vec<Command>>) {
        let mut input = InputReader::new();

        let mut unexecuted_commands: Vec<InputResult> = Vec::new();

        if let Some(cmds) = startup_commands {
            unexecuted_commands.append(&mut cmds.into_iter().map(|c| MetaCommand(c.clone())).collect());
        }


        loop {
            let res = if unexecuted_commands.len() > 0 { Ok(unexecuted_commands.remove(0)) } else { input.read_input() };

            match res {
                Ok(MetaCommand(Command::Read(files))) => {
                    for file in files {
                        let file_res = input.read_file(&file);
                        if let Ok(commands) = file_res {
                            unexecuted_commands.append(&mut commands.clone());
                        } else {
                            println!("{:?}", file_res.err());
                        }
                    }
                },
                Ok(MetaCommand(cmd)) => {
                    debug!("read command: {:?}", cmd);
                    self.handle_command(cmd);
                },
                Ok(Empty) |
                Ok(More) => (),
                Ok(Eof) => {
                    if input.is_tty() {
                        println!("");
                    }
                    break;
                },
                Err(e) => println!("{}", e.display()),
            }
        }
    }

    /// Runs a single command input.
    fn handle_command(&mut self, cmd: Command) {
        match cmd {
            Command::Help(args) => self.help_command(args),
            Command::Open(db) => {
                match self.store.open(Some(db.clone())) {
                    Ok(_) => println!("Database {:?} opened", db_output_name(&db)),
                    Err(e) => println!("{}", e.display())
                };
            },
            Command::Close => {
                let old_db_name = self.store.db_name.clone();
                match self.store.close() {
                    Ok(_) => println!("Database {:?} closed", db_output_name(&old_db_name)),
                    Err(e) => println!("{}", e.display())
                };
            },
            Command::Query(query) => self.execute_query(query),
            Command::Transact(transaction) => self.execute_transact(transaction),
            Command::Read(_) => (),
        }
    }

    fn help_command(&self, args: Vec<String>) {
        if args.is_empty() {
            for (cmd, msg) in COMMAND_HELP.iter() {
                println!(".{} - {}", cmd, msg);
            }
        } else {
            for mut arg in args {
                if arg.chars().nth(0).unwrap() == '.' { 
                    arg.remove(0);
                }
                let msg = COMMAND_HELP.get(arg.as_str());
                if msg.is_some() {
                    println!(".{} - {}", arg, msg.unwrap());
                } else {
                    println!("Unrecognised command {}", arg);
                }
            }
        }
    }

    fn execute_query(&self, query: String) {
        let results = match self.store.query(query){
            Result::Ok(vals) => {
                vals
            },
            Result::Err(err) => return println!("{}.", err.display()),
        };

        if results.is_empty() {
            return println!("No results found.")
        }
        if let Ok(output) = results.to_pretty() {
            println!("\n{}", output);
        }
    }

    fn execute_transact(&mut self, transaction: String) {
        match self.store.transact(transaction) {
            Result::Ok(report) => println!("{:?}", report),
            Result::Err(err) => println!("{}.", err.display()),
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
    }
}
