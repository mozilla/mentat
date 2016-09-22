// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

var datomish = require("../../target/release-node/datomish.js");

var schema = {
  "name": "pages",
  "attributes": [
    {"name":         "page/url",
     "type":         "string",
     "cardinality":  "one",
     "unique":       "identity",
     "doc":          "A page's URL."},
    {"name":         "page/title",
     "type":         "string",
     "cardinality":  "one",
     "doc":          "A page's title."},
    {"name":         "page/starred",
     "type":         "boolean",
     "cardinality":  "one",
     "doc":          "Whether the page is starred."},
    {"name":         "page/visit",
     "type":         "ref",
     "cardinality":  "many",
     "doc":          "A visit to the page."}
  ]
};

async function testOpen() {
  let db = await datomish.open("/tmp/testing.db");
  await db.ensureSchema(schema);
  let txResult = await db.transact([{"db/id": 55,
                                     "page/url": "http://foo.com/bar",
                                     "page/starred": true}]);
  console.log("Transaction returned " + JSON.stringify(txResult));
  console.log("Transaction instant: " + txResult.txInstant);
  let results = await datomish.q(db.db(), "[:find ?url :in $ :where [?e :page/url ?url]]")
  results = results.map(r => r[0]);
  console.log("Query results: " + JSON.stringify(results));
  await db.close();
}

testOpen()
.then((r) => console.log("Done."))
.catch((e) => console.log("Failure: " + e.stack));
