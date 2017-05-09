// Copyright 2017 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.
use input::{InputReader};
use input::InputResult::{Command, Empty, More, Eof};

/// Starting prompt
const DEFAULT_PROMPT: &'static str = "mentat=> ";
/// Prompt when further input is being read
// TODO: Should this actually reflect the current open brace?
const MORE_PROMPT: &'static str = "mentat.> ";

/// Executes input and maintains state of persistent items.
pub struct Repl {
}

impl Repl {
    /// Constructs a new `Repl`.
    pub fn new() -> Repl {
        Repl{}
    }

    /// Runs the REPL interactively.
    pub fn run(&mut self) {
        let mut more = false;
        let mut input = InputReader::new();

        loop {
            let res = input.read_input(if more { MORE_PROMPT } else { DEFAULT_PROMPT });

            match res {
                Command(name, args) => {
                    debug!("read command: {} {:?}", name, args);

                    more = false;
                    self.handle_command(name, args);
                },
                Empty => (),
                More => { more = true; },
                Eof => {
                    if input.is_tty() {
                        println!("");
                    }
                    break;
                }
            };
        }
    }

    /// Runs a single command input.
    fn handle_command(&mut self, cmd: String, args: Option<String>) {
        println!("{:?} {:?}", cmd, args);
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
    }
}
