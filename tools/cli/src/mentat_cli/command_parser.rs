// Copyright 2017 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use combine::{eof, many, many1, sep_by, skip_many, token, Parser};
use combine::combinator::{choice, try};
use combine::char::{alpha_num, space, string};

pub static HELP_COMMAND: &'static str = &"help";
pub static OPEN_COMMAND: &'static str = &"open";

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Command {
    Transact(Vec<String>),
    Query(Vec<String>),
    Help(Vec<String>),
    Open(String),
    Err(String),
}

impl Command {
    pub fn is_complete(&self) -> bool {
        match self {
            &Command::Query(_) |
            &Command::Transact(_) => false,
            _ => true
        }
    }
}

pub fn command(s: &str) -> Command {
    let help_parser = string(HELP_COMMAND).and(skip_many(space())).and(sep_by::<Vec<_>, _, _>(many1::<Vec<_>, _>(alpha_num()), token(' '))).map(|x| {
        let args: Vec<String> = x.1.iter().map(|v| v.iter().collect() ).collect();
        Command::Help(args)
    });

    let open_parser = string(OPEN_COMMAND).and(space()).and(many1::<Vec<_>, _>(alpha_num()).and(eof())).map(|x| {
        let arg: String = (x.1).0.iter().collect();
        Command::Open(arg)
    });

    token('.')
    .and(choice::<[&mut Parser<Input = _, Output = Command>; 2], _>
        ([&mut try(help_parser),
          &mut try(open_parser),]))
    .parse(s)
    .map(|x| x.0)
    .unwrap_or(('0', Command::Err(format!("Invalid command {:?}", s)))).1
}

#[cfg(test)]
mod tests {
    use super::*; 

    #[test]
    fn test_help_parser_multiple_args() {
        let input = ".help command1 command2";
        let cmd = command(&input);
        match cmd {
            Command::Help(args) => {
                assert_eq!(args, vec!["command1", "command2"]);
            },
            _ => assert!(false)
        }
    }

    #[test]
    fn test_help_parser_no_args() {
        let input = ".help";
        let cmd = command(&input);
        match cmd {
            Command::Help(args) => {
                let empty: Vec<String> = vec![];
                assert_eq!(args, empty);
            },
            _ => assert!(false)
        }
    }

    #[test]
    fn test_help_parser_no_args_trailing_whitespace() {
        let input = ".help ";
        let cmd = command(&input);
        match cmd {
            Command::Help(args) => {
                let empty: Vec<String> = vec![];
                assert_eq!(args, empty);
            },
            _ => assert!(false)
        }
    }

    #[test]
    fn test_open_parser_multiple_args() {
        let input = ".open database1 database2";
        let cmd = command(&input);
        match cmd {
            Command::Err(message) => {
                assert_eq!(message, format!("Invalid command {:?}", input));
            },
            _ => assert!(false)
        }
    }

    #[test]
    fn test_open_parser_single_arg() {
        let input = ".open database1";
        let cmd = command(&input);
        match cmd {
            Command::Open(arg) => {
                assert_eq!(arg, "database1".to_string());
            },
            _ => assert!(false)
        }
    }

    #[test]
    fn test_open_parser_no_args() {
        let input = ".open";
        let cmd = command(&input);
        match cmd {
            Command::Err(message) => {
                assert_eq!(message, format!("Invalid command {:?}", input));
            },
            _ => assert!(false)
        }
    }

    #[test]
    fn test_command_parser_no_dot() {
        let input = "help command1 command2";
        let cmd = command(&input);
        match cmd {
            Command::Err(message) => {
                assert_eq!(message, format!("Invalid command {:?}", input));
            },
            _ => assert!(false)
        }
    }

    #[test]
    fn test_command_parser_invalid_cmd() {
        let input = ".foo command1";
        let cmd = command(&input);
        match cmd {
            Command::Err(message) => {
                assert_eq!(message, format!("Invalid command {:?}", input));
            },
            _ => assert!(false)
        }
    
    }
}
