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
    Parser,
    any,
    eof,
    look_ahead,
    many1,
    satisfy,
    sep_end_by,
    token,
};

use combine::char::{
    space,
    spaces,
    string,
};

use combine::combinator::{
    choice,
    try,
};

use CliError;

use edn;

use failure::{
    Compat,
    Error,
};

use mentat::{
    CacheDirection,
};

#[macro_export]
macro_rules! bail {
    ($e:expr) => (
        return Err($e.into());
    )
}

pub static COMMAND_CACHE: &'static str = &"cache";
pub static COMMAND_CLOSE: &'static str = &"close";
pub static COMMAND_EXIT_LONG: &'static str = &"exit";
pub static COMMAND_EXIT_SHORT: &'static str = &"e";
pub static COMMAND_HELP: &'static str = &"help";
pub static COMMAND_IMPORT_LONG: &'static str = &"import";
pub static COMMAND_IMPORT_SHORT: &'static str = &"i";
pub static COMMAND_OPEN: &'static str = &"open";
pub static COMMAND_OPEN_EMPTY: &'static str = &"empty";
pub static COMMAND_OPEN_ENCRYPTED: &'static str = &"open_encrypted";
pub static COMMAND_OPEN_EMPTY_ENCRYPTED: &'static str = &"empty_encrypted";
pub static COMMAND_QUERY_LONG: &'static str = &"query";
pub static COMMAND_QUERY_SHORT: &'static str = &"q";
pub static COMMAND_QUERY_EXPLAIN_LONG: &'static str = &"explain_query";
pub static COMMAND_QUERY_EXPLAIN_SHORT: &'static str = &"eq";
pub static COMMAND_QUERY_PREPARED_LONG: &'static str = &"query_prepared";
pub static COMMAND_SCHEMA: &'static str = &"schema";
pub static COMMAND_SYNC: &'static str = &"sync";
pub static COMMAND_TIMER_LONG: &'static str = &"timer";
pub static COMMAND_TRANSACT_LONG: &'static str = &"transact";
pub static COMMAND_TRANSACT_SHORT: &'static str = &"t";

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Command {
    Cache(String, CacheDirection),
    Close,
    Exit,
    Help(Vec<String>),
    Import(String),
    Open(String),
    OpenEmpty(String),
    OpenEncrypted(String, String),
    OpenEmptyEncrypted(String, String),
    Query(String),
    QueryExplain(String),
    QueryPrepared(String),
    Schema,
    Sync(Vec<String>),
    Timer(bool),
    Transact(String),
}

impl Command {
    /// is_complete returns true if no more input is required for the command to be successfully executed.
    /// false is returned if the command is not considered valid.
    /// Defaults to true for all commands except Query and Transact.
    /// TODO: for query and transact commands, they will be considered complete if a parsable EDN has been entered as an argument
    pub fn is_complete(&self) -> bool {
        match self {
            &Command::Query(ref args) |
            &Command::QueryExplain(ref args) |
            &Command::QueryPrepared(ref args) |
            &Command::Transact(ref args)
            => {
                edn::parse::value(&args).is_ok()
            },
            &Command::Cache(_, _) |
            &Command::Close |
            &Command::Exit |
            &Command::Help(_) |
            &Command::Import(_) |
            &Command::Open(_) |
            &Command::OpenEmpty(_) |
            &Command::OpenEncrypted(_, _) |
            &Command::OpenEmptyEncrypted(_, _) |
            &Command::Timer(_) |
            &Command::Schema |
            &Command::Sync(_)
            => true,
        }
    }

    pub fn is_timed(&self) -> bool {
        match self {
            &Command::Import(_) |
            &Command::Query(_) |
            &Command::QueryPrepared(_) |
            &Command::Transact(_)
            => true,

            &Command::Cache(_, _) |
            &Command::Close |
            &Command::Exit |
            &Command::Help(_) |
            &Command::Open(_) |
            &Command::OpenEmpty(_) |
            &Command::OpenEncrypted(_, _) |
            &Command::OpenEmptyEncrypted(_, _) |
            &Command::QueryExplain(_) |
            &Command::Timer(_) |
            &Command::Schema |
            &Command::Sync(_)
            => false,
        }
    }

    pub fn output(&self) -> String {
        match self {
            &Command::Cache(ref attr, ref direction) => {
                format!(".{} {} {:?}", COMMAND_CACHE, attr, direction)
            },
            &Command::Close => {
                format!(".{}", COMMAND_CLOSE)
            },
            &Command::Exit => {
                format!(".{}", COMMAND_EXIT_LONG)
            },
            &Command::Help(ref args) => {
                format!(".{} {:?}", COMMAND_HELP, args)
            },
            &Command::Import(ref args) => {
               format!(".{} {}", COMMAND_IMPORT_LONG, args)
            },
            &Command::Open(ref args) => {
                format!(".{} {}", COMMAND_OPEN, args)
            },
            &Command::OpenEmpty(ref args) => {
                format!(".{} {}", COMMAND_OPEN_EMPTY, args)
            },
            &Command::OpenEncrypted(ref db, ref key) => {
                format!(".{} {} {}", COMMAND_OPEN_ENCRYPTED, db, key)
            },
            &Command::OpenEmptyEncrypted(ref db, ref key) => {
                format!(".{} {} {}", COMMAND_OPEN_EMPTY_ENCRYPTED, db, key)
            },
            &Command::Query(ref args) => {
                format!(".{} {}", COMMAND_QUERY_LONG, args)
            },
            &Command::QueryExplain(ref args) => {
                format!(".{} {}", COMMAND_QUERY_EXPLAIN_LONG, args)
            },
            &Command::QueryPrepared(ref args) => {
                format!(".{} {}", COMMAND_QUERY_PREPARED_LONG, args)
            },
            &Command::Schema => {
                format!(".{}", COMMAND_SCHEMA)
            },
            &Command::Sync(ref args) => {
                format!(".{} {:?}", COMMAND_SYNC, args)
            },
            &Command::Timer(on) => {
                format!(".{} {}", COMMAND_TIMER_LONG, on)
            },
            &Command::Transact(ref args) => {
                format!(".{} {}", COMMAND_TRANSACT_LONG, args)
            },
        }
    }
}

pub fn command(s: &str) -> Result<Command, Error> {
    let path = || many1::<String, _>(satisfy(|c: char| !c.is_whitespace()));
    let argument = || many1::<String, _>(satisfy(|c: char| !c.is_whitespace()));
    let arguments = || sep_end_by::<Vec<_>, _, _>(many1(satisfy(|c: char| !c.is_whitespace())), many1::<Vec<_>, _>(space())).expected("arguments");

    // Helpers.
    let direction_parser = || string("forward")
                                .map(|_| CacheDirection::Forward)
                           .or(string("reverse").map(|_| CacheDirection::Reverse))
                           .or(string("both").map(|_| CacheDirection::Both));

    let edn_arg_parser = || spaces()
                            .with(look_ahead(string("[").or(string("{")))
                                .with(many1::<Vec<_>, _>(try(any())))
                                .and_then(|args| -> Result<String, Compat<Error>> {
                                    Ok(args.iter().collect())
                                })
                            );

    let no_arg_parser = || arguments()
                        .skip(spaces())
                        .skip(eof());

    let opener = |command, num_args| {
        string(command)
            .with(spaces())
            .with(arguments())
            .map(move |args| {
                if args.len() < num_args {
                    bail!(CliError::CommandParse("Missing required argument".to_string()));
                }
                if args.len() > num_args {
                    bail!(CliError::CommandParse(format!("Unrecognized argument {:?}", args[num_args])));
                }
                Ok(args)
            })
    };

    // Commands.
    let cache_parser = string(COMMAND_CACHE)
                    .with(spaces())
                    .with(argument().skip(spaces()).and(direction_parser())
                    .map(|(arg, direction)| {
                        Ok(Command::Cache(arg, direction))
                    }));


    let close_parser = string(COMMAND_CLOSE)
                    .with(no_arg_parser())
                    .map(|args| {
                        if !args.is_empty() {
                            bail!(CliError::CommandParse(format!("Unrecognized argument {:?}", args[0])) );
                        }
                        Ok(Command::Close)
                    });

    let exit_parser = try(string(COMMAND_EXIT_LONG)).or(try(string(COMMAND_EXIT_SHORT)))
                    .with(no_arg_parser())
                    .map(|args| {
                        if !args.is_empty() {
                            bail!(CliError::CommandParse(format!("Unrecognized argument {:?}", args[0])) );
                        }
                        Ok(Command::Exit)
                    });

    let explain_query_parser = try(string(COMMAND_QUERY_EXPLAIN_LONG))
                           .or(try(string(COMMAND_QUERY_EXPLAIN_SHORT)))
                        .with(edn_arg_parser())
                        .map(|x| {
                            Ok(Command::QueryExplain(x))
                        });

    let help_parser = string(COMMAND_HELP)
                    .with(spaces())
                    .with(arguments())
                    .map(|args| {
                        Ok(Command::Help(args.clone()))
                    });

    let import_parser = try(string(COMMAND_IMPORT_LONG)).or(try(string(COMMAND_IMPORT_SHORT)))
                    .with(spaces())
                    .with(path())
                    .map(|x| {
                        Ok(Command::Import(x))
                    });

    let open_parser = opener(COMMAND_OPEN, 1).map(|args_res|
        args_res.map(|args| Command::Open(args[0].clone())));

    let open_empty_parser = opener(COMMAND_OPEN_EMPTY, 1).map(|args_res|
        args_res.map(|args| Command::OpenEmpty(args[0].clone())));

    let open_encrypted_parser = opener(COMMAND_OPEN_ENCRYPTED, 2).map(|args_res|
        args_res.map(|args| Command::OpenEncrypted(args[0].clone(), args[1].clone())));

    let open_empty_encrypted_parser = opener(COMMAND_OPEN_EMPTY_ENCRYPTED, 2).map(|args_res|
        args_res.map(|args| Command::OpenEmptyEncrypted(args[0].clone(), args[1].clone())));

    let query_parser = try(string(COMMAND_QUERY_LONG)).or(try(string(COMMAND_QUERY_SHORT)))
                        .with(edn_arg_parser())
                        .map(|x| {
                            Ok(Command::Query(x))
                        });

    let query_prepared_parser = string(COMMAND_QUERY_PREPARED_LONG)
                        .with(edn_arg_parser())
                        .map(|x| {
                            Ok(Command::QueryPrepared(x))
                        });

    let schema_parser = string(COMMAND_SCHEMA)
                    .with(no_arg_parser())
                    .map(|args| {
                        if !args.is_empty() {
                            bail!(CliError::CommandParse(format!("Unrecognized argument {:?}", args[0])) );
                        }
                        Ok(Command::Schema)
                    });

    let sync_parser = string(COMMAND_SYNC)
                    .with(spaces())
                    .with(arguments())
                    .map(|args| {
                        if args.len() < 1 {
                            bail!(CliError::CommandParse("Missing required argument".to_string()));
                        }
                        if args.len() > 2 {
                            bail!(CliError::CommandParse(format!("Unrecognized argument {:?}", args[2])));
                        }
                        Ok(Command::Sync(args.clone()))
                    });

    let timer_parser = string(COMMAND_TIMER_LONG)
                    .with(spaces())
                    .with(string("on").map(|_| true).or(string("off").map(|_| false)))
                    .map(|args| {
                        Ok(Command::Timer(args))
                    });

    let transact_parser = try(string(COMMAND_TRANSACT_LONG)).or(try(string(COMMAND_TRANSACT_SHORT)))
                    .with(edn_arg_parser())
                    .map(|x| {
                        Ok(Command::Transact(x))
                    });

    spaces()
    .skip(token('.'))
    .with(choice::<[&mut Parser<Input = _, Output = Result<Command, Error>>; 16], _>
          ([&mut try(help_parser),
            &mut try(import_parser),
            &mut try(timer_parser),
            &mut try(cache_parser),
            &mut try(open_encrypted_parser),
            &mut try(open_empty_encrypted_parser),
            &mut try(open_parser),
            &mut try(open_empty_parser),
            &mut try(close_parser),
            &mut try(explain_query_parser),
            &mut try(exit_parser),
            &mut try(query_prepared_parser),
            &mut try(query_parser),
            &mut try(schema_parser),
            &mut try(sync_parser),
            &mut try(transact_parser)]))
        .parse(s)
        .unwrap_or((Err(CliError::CommandParse(format!("Invalid command {:?}", s)).into()), "")).0
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
    fn test_open_encrypted_parser() {
        let input = ".open_encrypted /path/to/my.db hunter2";
        let cmd = command(&input).expect("Expected open_encrypted command");
        match cmd {
            Command::OpenEncrypted(path, key) => {
                assert_eq!(path, "/path/to/my.db".to_string());
                assert_eq!(key, "hunter2".to_string());
            },
            _ => assert!(false)
        }
    }

    #[test]
    fn test_empty_encrypted_parser() {
        let input = ".empty_encrypted /path/to/my.db hunter2";
        let cmd = command(&input).expect("Expected empty_encrypted command");
        match cmd {
            Command::OpenEmptyEncrypted(path, key) => {
                assert_eq!(path, "/path/to/my.db".to_string());
                assert_eq!(key, "hunter2".to_string());
            },
            _ => assert!(false)
        }
    }

    #[test]
    fn test_open_encrypted_parser_missing_key() {
        let input = ".open_encrypted path/to/db.db";
        let err = command(&input).expect_err("Expected an error");
        assert_eq!(err.to_string(), "Missing required argument");
    }

    #[test]
    fn test_empty_encrypted_parser_missing_key() {
        let input = ".empty_encrypted path/to/db.db";
        let err = command(&input).expect_err("Expected an error");
        assert_eq!(err.to_string(), "Missing required argument");
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
    fn test_import_parser() {
        let input = ".import /foo/bar/";
        let cmd = command(&input).expect("Expected import command");
        match cmd {
            Command::Import(path) => assert_eq!(path, "/foo/bar/"),
            _ => panic!("Wrong command!")
        }
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
