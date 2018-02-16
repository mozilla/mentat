// Copyright 2017 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use combine::{
    any,
    eof,
    look_ahead,
    many1,
    satisfy,
    sep_end_by,
    token,
    Parser
};
use combine::char::{
    space,
    spaces,
    string
};
use combine::combinator::{
    choice,
    try
};

use errors as cli;

use edn;

pub static HELP_COMMAND: &'static str = &"help";
pub static OPEN_COMMAND: &'static str = &"open";
pub static CLOSE_COMMAND: &'static str = &"close";
pub static LONG_QUERY_COMMAND: &'static str = &"query";
pub static SHORT_QUERY_COMMAND: &'static str = &"q";
pub static SCHEMA_COMMAND: &'static str = &"schema";
pub static LONG_TIMER_COMMAND: &'static str = &"timer";
pub static LONG_TRANSACT_COMMAND: &'static str = &"transact";
pub static SHORT_TRANSACT_COMMAND: &'static str = &"t";
pub static LONG_EXIT_COMMAND: &'static str = &"exit";
pub static SHORT_EXIT_COMMAND: &'static str = &"e";
pub static LONG_QUERY_EXPLAIN_COMMAND: &'static str = &"explain_query";
pub static SHORT_QUERY_EXPLAIN_COMMAND: &'static str = &"eq";
pub static SYNC_COMMAND: &'static str = &"sync";

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Command {
    Close,
    Exit,
    Help(Vec<String>),
    Open(String),
    Query(String),
    Schema,
    Sync(Vec<String>),
    Timer(bool),
    Transact(String),
    QueryExplain(String),
}

impl Command {
    /// is_complete returns true if no more input is required for the command to be successfully executed.
    /// false is returned if the command is not considered valid.
    /// Defaults to true for all commands except Query and Transact.
    /// TODO: for query and transact commands, they will be considered complete if a parsable EDN has been entered as an argument
    pub fn is_complete(&self) -> bool {
        match self {
            &Command::Query(ref args) |
            &Command::Transact(ref args) |
            &Command::QueryExplain(ref args) => {
                edn::parse::value(&args).is_ok()
            },
            &Command::Timer(_) |
            &Command::Help(_) |
            &Command::Open(_) |
            &Command::Close |
            &Command::Exit |
            &Command::Sync(_) |
            &Command::Schema => true
        }
    }

    pub fn is_timed(&self) -> bool {
        match self {
            &Command::Query(_) |
            &Command::Transact(_)  => true,
            &Command::QueryExplain(_) |
            &Command::Timer(_) |
            &Command::Help(_) |
            &Command::Open(_) |
            &Command::Close |
            &Command::Exit |
            &Command::Schema => false
        }
    }

    pub fn output(&self) -> String {
        match self {
            &Command::Query(ref args) => {
                format!(".{} {}", LONG_QUERY_COMMAND, args)
            },
            &Command::Transact(ref args) => {
                format!(".{} {}", LONG_TRANSACT_COMMAND, args)
            },
            &Command::Timer(on) => {
                format!(".{} {}", LONG_TIMER_COMMAND, on)
            },
            &Command::Help(ref args) => {
                format!(".{} {:?}", HELP_COMMAND, args)
            },
            &Command::Open(ref args) => {
                format!(".{} {}", OPEN_COMMAND, args)
            },
            &Command::Close => {
                format!(".{}", CLOSE_COMMAND)
            },
            &Command::Exit => {
                format!(".{}", LONG_EXIT_COMMAND)
            },
            &Command::Schema => {
                format!(".{}", SCHEMA_COMMAND)
            },
            &Command::Sync(ref args) => {
                format!(".{} {:?}", SYNC_COMMAND, args)
            },
            &Command::QueryExplain(ref args) => {
                format!(".{} {}", LONG_QUERY_EXPLAIN_COMMAND, args)
            },
        }
    }
}

pub fn command(s: &str) -> Result<Command, cli::Error> {
    let arguments = || sep_end_by::<Vec<_>, _, _>(many1(satisfy(|c: char| !c.is_whitespace())), many1::<Vec<_>, _>(space())).expected("arguments");

    let help_parser = string(HELP_COMMAND)
                    .with(spaces())
                    .with(arguments())
                    .map(|args| {
                        Ok(Command::Help(args.clone()))
                    });

    let timer_parser = string(LONG_TIMER_COMMAND)
                    .with(spaces())
                    .with(string("on").map(|_| true).or(string("off").map(|_| false)))
                    .map(|args| {
                        Ok(Command::Timer(args))
                    });

    let open_parser = string(OPEN_COMMAND)
                    .with(spaces())
                    .with(arguments())
                    .map(|args| {
                        if args.len() < 1 {
                            bail!(cli::ErrorKind::CommandParse("Missing required argument".to_string()));
                        }
                        if args.len() > 1 {
                            bail!(cli::ErrorKind::CommandParse(format!("Unrecognized argument {:?}", args[1])));
                        }
                        Ok(Command::Open(args[0].clone()))
                    });

    let no_arg_parser = || arguments()
                        .skip(spaces())
                        .skip(eof());

    let close_parser = string(CLOSE_COMMAND)
                    .with(no_arg_parser())
                    .map(|args| {
                        if !args.is_empty() {
                            bail!(cli::ErrorKind::CommandParse(format!("Unrecognized argument {:?}", args[0])) );
                        }
                        Ok(Command::Close)
                    });

    let schema_parser = string(SCHEMA_COMMAND)
                    .with(no_arg_parser())
                    .map(|args| {
                        if !args.is_empty() {
                            bail!(cli::ErrorKind::CommandParse(format!("Unrecognized argument {:?}", args[0])) );
                        }
                        Ok(Command::Schema)
                    });

    let sync_parser = string(SYNC_COMMAND)
                    .with(spaces())
                    .with(arguments())
                    .map(|args| {
                        if args.len() < 1 {
                            bail!(cli::ErrorKind::CommandParse("Missing required argument".to_string()));
                        }
                        if args.len() > 2 {
                            bail!(cli::ErrorKind::CommandParse(format!("Unrecognized argument {:?}", args[2])));
                        }
                        Ok(Command::Sync(args.clone()))
                    });

    let exit_parser = try(string(LONG_EXIT_COMMAND)).or(try(string(SHORT_EXIT_COMMAND)))
                    .with(no_arg_parser())
                    .map(|args| {
                        if !args.is_empty() {
                            bail!(cli::ErrorKind::CommandParse(format!("Unrecognized argument {:?}", args[0])) );
                        }
                        Ok(Command::Exit)
                    });

    let edn_arg_parser = || spaces()
                            .with(look_ahead(string("[").or(string("{")))
                                .with(many1::<Vec<_>, _>(try(any())))
                                .and_then(|args| -> Result<String, cli::Error> {
                                    Ok(args.iter().collect())
                                })
                            );

    let query_parser = try(string(LONG_QUERY_COMMAND)).or(try(string(SHORT_QUERY_COMMAND)))
                        .with(edn_arg_parser())
                        .map(|x| {
                            Ok(Command::Query(x))
                        });

    let transact_parser = try(string(LONG_TRANSACT_COMMAND)).or(try(string(SHORT_TRANSACT_COMMAND)))
                    .with(edn_arg_parser())
                    .map( |x| {
                        Ok(Command::Transact(x))
                    });

    let explain_query_parser = try(string(LONG_QUERY_EXPLAIN_COMMAND))
                           .or(try(string(SHORT_QUERY_EXPLAIN_COMMAND)))
                        .with(edn_arg_parser())
                        .map(|x| {
                            Ok(Command::QueryExplain(x))
                        });
    spaces()
    .skip(token('.'))
    .with(choice::<[&mut Parser<Input = _, Output = Result<Command, cli::Error>>; 9], _>
          ([&mut try(help_parser),
            &mut try(timer_parser),
            &mut try(open_parser),
            &mut try(close_parser),
            &mut try(explain_query_parser),
            &mut try(exit_parser),
            &mut try(query_parser),
            &mut try(schema_parser),
            &mut try(sync_parser),
            &mut try(transact_parser)]))
        .parse(s)
        .unwrap_or((Err(cli::ErrorKind::CommandParse(format!("Invalid command {:?}", s)).into()), "")).0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_help_parser_multiple_args() {
        let input = ".help command1 command2";
        let cmd = command(&input).expect("Expected help command");
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
        let cmd = command(&input).expect("Expected help command");
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
        let cmd = command(&input).expect("Expected help command");
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
        let cmd = command(&input).expect("Expected help command");
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
        let err = command(&input).expect_err("Expected an error");
        assert_eq!(err.to_string(), "Unrecognized argument \"database2\"");
    }

    #[test]
    fn test_open_parser_single_arg() {
        let input = ".open database1";
        let cmd = command(&input).expect("Expected open command");
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
        let cmd = command(&input).expect("Expected open command");
        match cmd {
            Command::Open(arg) => {
                assert_eq!(arg, "/path/to/my.db".to_string());
            },
            _ => assert!(false)
        }
    }

    #[test]
    fn test_sync_parser_path_arg() {
        let input = ".sync https://example.com/api/ 316ea470-ce35-4adf-9c61-e0de6e289c59";
        let cmd = command(&input).expect("Expected open command");
        match cmd {
            Command::Sync(args) => {
                assert_eq!(args[0], "https://example.com/api/".to_string());
                assert_eq!(args[1], "316ea470-ce35-4adf-9c61-e0de6e289c59".to_string());
            },
            _ => assert!(false)
        }
    }

    #[test]
    fn test_open_parser_file_arg() {
        let input = ".open my.db";
        let cmd = command(&input).expect("Expected open command");
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
        let err = command(&input).expect_err("Expected an error");
        assert_eq!(err.to_string(), "Missing required argument");
    }

    #[test]
    fn test_open_parser_no_args_trailing_whitespace() {
        let input = ".open ";
        let err = command(&input).expect_err("Expected an error");
        assert_eq!(err.to_string(), "Missing required argument");
    }

    #[test]
    fn test_close_parser_with_args() {
        let input = ".close arg1";
        let err = command(&input).expect_err("Expected an error");
        assert_eq!(err.to_string(), format!("Invalid command {:?}", input));
    }

    #[test]
    fn test_close_parser_no_args() {
        let input = ".close";
        let cmd = command(&input).expect("Expected close command");
        match cmd {
            Command::Close => assert!(true),
            _ => assert!(false)
        }
    }

    #[test]
    fn test_close_parser_no_args_trailing_whitespace() {
        let input = ".close ";
        let cmd = command(&input).expect("Expected close command");
        match cmd {
            Command::Close => assert!(true),
            _ => assert!(false)
        }
    }

    #[test]
    fn test_exit_parser_with_args() {
        let input = ".exit arg1";
        let err = command(&input).expect_err("Expected an error");
        assert_eq!(err.to_string(), format!("Invalid command {:?}", input));
    }

    #[test]
    fn test_exit_parser_no_args() {
        let input = ".exit";
        let cmd = command(&input).expect("Expected exit command");
        match cmd {
            Command::Exit => assert!(true),
                        _ => assert!(false)
        }
    }

    #[test]
    fn test_exit_parser_no_args_trailing_whitespace() {
        let input = ".exit ";
        let cmd = command(&input).expect("Expected exit command");
        match cmd {
            Command::Exit => assert!(true),
            _ => assert!(false)
        }
    }

    #[test]
    fn test_exit_parser_short_command() {
        let input = ".e";
        let cmd = command(&input).expect("Expected exit command");
        match cmd {
            Command::Exit => assert!(true),
            _ => assert!(false)
        }
    }

    #[test]
    fn test_schema_parser_with_args() {
        let input = ".schema arg1";
        let err = command(&input).expect_err("Expected an error");
        assert_eq!(err.to_string(), format!("Invalid command {:?}", input));
    }

    #[test]
    fn test_schema_parser_no_args() {
        let input = ".schema";
        let cmd = command(&input).expect("Expected schema command");
        match cmd {
            Command::Schema => assert!(true),
            _ => assert!(false)
        }
    }

    #[test]
    fn test_schema_parser_no_args_trailing_whitespace() {
        let input = ".schema ";
        let cmd = command(&input).expect("Expected schema command");
        match cmd {
            Command::Schema => assert!(true),
            _ => assert!(false)
        }
    }

    #[test]
    fn test_query_parser_complete_edn() {
        let input = ".q [:find ?x :where [?x foo/bar ?y]]";
        let cmd = command(&input).expect("Expected query command");
        match cmd {
            Command::Query(edn) => assert_eq!(edn, "[:find ?x :where [?x foo/bar ?y]]"),
            _ => assert!(false)
        }
    }

    #[test]
    fn test_query_parser_alt_query_command() {
        let input = ".query [:find ?x :where [?x foo/bar ?y]]";
        let cmd = command(&input).expect("Expected query command");
        match cmd {
            Command::Query(edn) => assert_eq!(edn, "[:find ?x :where [?x foo/bar ?y]]"),
            _ => assert!(false)
        }
    }

    #[test]
    fn test_query_parser_incomplete_edn() {
        let input = ".q [:find ?x\r\n";
        let cmd = command(&input).expect("Expected query command");
        match cmd {
            Command::Query(edn) => assert_eq!(edn, "[:find ?x\r\n"),
            _ => assert!(false)
        }
    }

    #[test]
    fn test_query_parser_empty_edn() {
        let input = ".q {}";
        let cmd = command(&input).expect("Expected query command");
        match cmd {
            Command::Query(edn) => assert_eq!(edn, "{}"),
            _ => assert!(false)
        }
    }

    #[test]
    fn test_query_parser_no_edn() {
        let input = ".q ";
        let err = command(&input).expect_err("Expected an error");
        assert_eq!(err.to_string(), format!("Invalid command {:?}", input));
    }

    #[test]
    fn test_query_parser_invalid_start_char() {
        let input = ".q :find ?x";
        let err = command(&input).expect_err("Expected an error");
        assert_eq!(err.to_string(), format!("Invalid command {:?}", input));
    }

    #[test]
    fn test_transact_parser_complete_edn() {
        let input = ".t [[:db/add \"s\" :db/ident :foo/uuid] [:db/add \"r\" :db/ident :bar/uuid]]";
        let cmd = command(&input).expect("Expected transact command");
        match cmd {
            Command::Transact(edn) => assert_eq!(edn, "[[:db/add \"s\" :db/ident :foo/uuid] [:db/add \"r\" :db/ident :bar/uuid]]"),
            _ => assert!(false)
        }
    }

    #[test]
    fn test_transact_parser_alt_command() {
        let input = ".transact [[:db/add \"s\" :db/ident :foo/uuid] [:db/add \"r\" :db/ident :bar/uuid]]";
        let cmd = command(&input).expect("Expected transact command");
        match cmd {
            Command::Transact(edn) => assert_eq!(edn, "[[:db/add \"s\" :db/ident :foo/uuid] [:db/add \"r\" :db/ident :bar/uuid]]"),
            _ => assert!(false)
        }
    }

    #[test]
    fn test_transact_parser_incomplete_edn() {
        let input = ".t {\r\n";
        let cmd = command(&input).expect("Expected transact command");
        match cmd {
            Command::Transact(edn) => assert_eq!(edn, "{\r\n"),
            _ => assert!(false)
        }
    }

    #[test]
    fn test_transact_parser_empty_edn() {
        let input = ".t {}";
        let cmd = command(&input).expect("Expected transact command");
        match cmd {
            Command::Transact(edn) => assert_eq!(edn, "{}"),
            _ => assert!(false)
        }
    }

    #[test]
    fn test_transact_parser_no_edn() {
        let input = ".t ";
        let err = command(&input).expect_err("Expected an error");
        assert_eq!(err.to_string(), format!("Invalid command {:?}", input));
    }

    #[test]
    fn test_transact_parser_invalid_start_char() {
        let input = ".t :db/add \"s\" :db/ident :foo/uuid";
        let err = command(&input).expect_err("Expected an error");
        assert_eq!(err.to_string(), format!("Invalid command {:?}", input));
    }

    #[test]
    fn test_parser_preceeding_trailing_whitespace() {
        let input = " .close ";
        let cmd = command(&input).expect("Expected close command");
        match cmd {
            Command::Close => assert!(true),
            _ => assert!(false)
        }
    }

    #[test]
    fn test_command_parser_no_dot() {
        let input = "help command1 command2";
        let err = command(&input).expect_err("Expected an error");
        assert_eq!(err.to_string(), format!("Invalid command {:?}", input));
    }

    #[test]
    fn test_command_parser_invalid_cmd() {
        let input = ".foo command1";
        let err = command(&input).expect_err("Expected an error");
        assert_eq!(err.to_string(), format!("Invalid command {:?}", input));
    }
}
