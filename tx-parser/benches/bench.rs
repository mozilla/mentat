#![feature(test)]

// These benchmarks can be run from the project root with:
// > cargo bench --package mentat_tx_parser

extern crate test;
extern crate edn;
extern crate mentat_tx_parser;

use test::Bencher;
use mentat_tx_parser::Tx;

#[bench]
fn bench_parse1(b: &mut Bencher) {
    let input = r#"[[:db/add 1 :test/val "a"]]"#;
    let parsed_edn = edn::parse::value(input).expect("to parse test input");
    b.iter(|| Tx::parse(&parsed_edn));
}

#[bench]
fn bench_parse2(b: &mut Bencher) {
    let input = r#"
        [[:db/add 1  :test/val "a"]
         [:db/add 2  :test/val "b"]
         [:db/add 3  :test/val "c"]
         [:db/add 4  :test/val "d"]
         [:db/add 5  :test/val "e"]
         [:db/add 6  :test/val "f"]
         [:db/add 7  :test/val "g"]
         [:db/add 8  :test/val "h"]
         [:db/add 9  :test/val "i"]
         [:db/add 10 :test/val "j"]
         [:db/add 11 :test/val "k"]
         [:db/add 12 :test/val "l"]
         [:db/add 13 :test/val "m"]
         [:db/add 14 :test/val "n"]
         [:db/add 15 :test/val "o"]
         [:db/add 16 :test/val "p"]
         [:db/add 17 :test/val "q"]
         [:db/add 18 :test/val "r"]
         [:db/add 19 :test/val "s"]
         [:db/add 20 :test/val "t"]
         [:db/add 21 :test/val "u"]
         [:db/add 22 :test/val "v"]
         [:db/add 23 :test/val "w"]
         [:db/add 24 :test/val "x"]
         [:db/add 25 :test/val "y"]
         [:db/add 26 :test/val "z"]]"#;
    b.iter(|| {
        let parsed_edn = edn::parse::value(input).expect("to parse test input");
        Tx::parse(&parsed_edn).expect("to parse tx");
    });
}

#[bench]
fn bench_parse3(b: &mut Bencher) {
    let input = r#"
        [[:db/add 1  :test/val "a"]
         [:db/add 2  :test/val "b"]
         [:db/add 3  :test/val "c"]
         [:db/add 4  :test/val "d"]
         [:db/add 5  :test/val "e"]
         [:db/add 6  :test/val "f"]
         [:db/add 7  :test/val "g"]
         [:db/add 8  :test/val "h"]
         [:db/add 9  :test/val "i"]
         [:db/add 10 :test/val "j"]
         [:db/add 11 :test/val "k"]
         [:db/add 12 :test/val "l"]
         [:db/add 13 :test/val "m"]
         [:db/add 14 :test/val "n"]
         [:db/add 15 :test/val "o"]
         [:db/add 16 :test/val "p"]
         [:db/add 17 :test/val "q"]
         [:db/add 18 :test/val "r"]
         [:db/add 19 :test/val "s"]
         [:db/add 20 :test/val "t"]
         [:db/add 21 :test/val "u"]
         [:db/add 22 :test/val "v"]
         [:db/add 23 :test/val "w"]
         [:db/add 24 :test/val "x"]
         [:db/add 25 :test/val "y"]
         [:db/add 26 :test/val "z"]]"#;
    b.iter(|| {
        edn::parse::entities(input).expect("to parse test input");
    });
}
