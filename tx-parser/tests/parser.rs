// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate edn;
extern crate combine;
extern crate mentat_tx;
extern crate mentat_tx_parser;

use edn::parse;
use edn::symbols::NamespacedKeyword;
use edn::types::Value;
use mentat_tx::entities::*;
use mentat_tx_parser::Tx;

#[test]
fn test_entities() {

    // TODO: align with whitespace after the EDN parser ignores more whitespace.
    let input = r#"[[:db/add 101 :test/a "v"]
[:db/retract 102 :test/b "w"]]"#;

    let edn = parse::value(input).unwrap();
    let input = [edn];

    let result = Tx::parse(&input[..]);
    assert_eq!(result,
               Ok(vec![
                   Entity::Add {
                       e: EntidOrLookupRef::Entid(Entid::Entid(101)),
                       a: Entid::Ident(NamespacedKeyword::new("test", "a")),
                       v: ValueOrLookupRef::Value(Value::Text("v".into())),
                       tx: None,
                   },
                   Entity::Retract {
                       e: EntidOrLookupRef::Entid(Entid::Entid(102)),
                       a: Entid::Ident(NamespacedKeyword::new("test", "b")),
                       v: ValueOrLookupRef::Value(Value::Text("w".into())),
                   },
                   ]));
}

// TODO: test error handling in select cases.
