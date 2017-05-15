// Copyright 2017 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use combine::{any, eof, many, many1, sep_by, skip_many, token, Parser};
use combine::combinator::{choice, try};
use combine::char::{space, string};

pub static HELP_COMMAND: &'static str = &"help";
pub static OPEN_COMMAND: &'static str = &"open";
pub static CLOSE_COMMAND: &'static str = &"close";

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Command {
    Transact(Vec<String>),
    Query(Vec<String>),
    Help(Vec<String>),
    Open(String),
    Close,
    Err(String),
}

impl Command {
    pub fn is_complete(&self) -> bool {
        match self {
            &Command::Query(_) |
            &Command::Transact(_) => false,
            &Command::Help(_) |
            &Command::Open(_) |
            &Command::Close |
            &Command::Err(_) => true
        }
    }
}

pub fn command(s: &str) -> Command {
    let help_parser = string(HELP_COMMAND)
                    .and(skip_many(space()))
                    .and(many::<Vec<_>, _>(try(any())))
                    .map(|x| {
                        let remainder: String = x.1.iter().collect();
                        let args: Vec<String> = remainder.split(" ").filter_map(|s| if s.is_empty() { None } else { Some(s.to_string())}).collect();
                        Command::Help(args)
                    });

    let open_parser = string(OPEN_COMMAND)
                    .and(skip_many(space()))
                    .and(many1::<Vec<_>, _>(try(any())))
                    .map(|x| {
                        let remainder: String = x.1.iter().collect();
                        let args: Vec<String> = remainder.split(" ").filter_map(|s| if s.is_empty() { None } else { Some(s.to_string())}).collect();
                        if args.len() > 1 {
                            return Command::Err(format!("Unrecognized argument {:?}", (&args[1]).clone()));
                        }
                        Command::Open((&args[0]).clone())
                    });

    let close_parser = string(CLOSE_COMMAND)
                    .and(skip_many(space()))
                    .map( |_| Command::Close );

    skip_many(space())
    .and(token('.'))
    .and(choice::<[&mut Parser<Input = _, Output = Command>; 3], _>
        ([&mut try(help_parser),
          &mut try(open_parser),
          &mut try(close_parser),]))
    .skip(eof())
    .parse(s)
    .map(|x| x.0)
    .unwrap_or((((), '0'), Command::Err(format!("Invalid command {:?}", s)))).1
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
    fn test_help_parser_dot_arg() {
        let input = ".help .command1";
        let cmd = command(&input);
        match cmd {
            Command::Help(args) => {
                assert_eq!(args, vec![".command1"]);
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
                assert_eq!(message, "Unrecognized argument \"database2\"");
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
    fn test_open_parser_path_arg() {
        let input = ".open /path/to/my.db";
        let cmd = command(&input);
        match cmd {
            Command::Open(arg) => {
                assert_eq!(arg, "/path/to/my.db".to_string());
            },
            _ => assert!(false)
        }
    }

    #[test]
    fn test_open_parser_file_arg() {
        let input = ".open my.db";
        let cmd = command(&input);
        match cmd {
            Command::Open(arg) => {
                assert_eq!(arg, "my.db".to_string());
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
    fn test_close_parser_with_args() {
        let input = ".close arg1";
        let cmd = command(&input);
        match cmd {
            Command::Err(message) => {
                assert_eq!(message, format!("Invalid command {:?}", input));
            },
            _ => assert!(false)
        }
    }

    #[test]
    fn test_close_parser_no_args() {
        let input = ".close";
        let cmd = command(&input);
        match cmd {
            Command::Close => assert!(true),
            _ => assert!(false)
        }
    }

    #[test]
    fn test_close_parser_no_args_trailing_whitespace() {
        let input = ".close ";
        let cmd = command(&input);
        match cmd {
            Command::Close => assert!(true),
            _ => assert!(false)
        }
    }

    #[test]
    fn test_parser_preceeding_trailing_whitespace() {
        let input = " .close ";
        let cmd = command(&input);
        match cmd {
            Command::Close => assert!(true),
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
