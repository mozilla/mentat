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
     "doc":          "A page's title."}
  ]
};

async function testOpen() {
  // Open a database.
  let db = await datomish.open("/tmp/testing.db");

  // Make sure we have our current schema.
  await db.ensureSchema(schema);

  // Add some data. Note that we use a temporary ID (the real ID
  // will be assigned by Datomish).
  let txResult = await db.transact([
    {"db/id": datomish.tempid(),
     "page/url": "https://mozilla.org/",
     "page/title": "Mozilla"}
  ]);

  console.log("Transaction returned " + JSON.stringify(txResult));
  console.log("Transaction instant: " + txResult.txInstant);

  // A simple query.
  let results = await db.q("[:find [?url ...] :in $ :where [?e :page/url ?url]]");
  console.log("Query results: " + JSON.stringify(results));

  // Let's extend our schema. In the real world this would typically happen
  // across releases.
  schema.attributes.push({"name":        "page/visitedAt",
                          "type":        "instant",
                          "cardinality": "many",
                          "doc":         "A visit to the page."});
  await db.ensureSchema(schema);

  // Now we can make assertions with the new vocabulary about existing
  // entities.
  // Note that we simply let Datomish find which page we're talking about by
  // URL -- the URL is a unique property -- so we just use a tempid again.
  await db.transact([
    {"db/id": datomish.tempid(),
     "page/url": "https://mozilla.org/",
     "page/visitedAt": (new Date())}
  ]);

  // When did we most recently visit this page?
  let date = (await db.q(
    `[:find (max ?date) .
      :in $ ?url
      :where
      [?page :page/url ?url]
      [?page :page/visitedAt ?date]]`,
    {"inputs": {"url": "https://mozilla.org/"}}));
  console.log("Most recent visit: " + date);

  // Close: we're done!
  await db.close();
}

testOpen()
.then((r) => console.log("Done."))
.catch((e) => console.log("Failure: " + e.stack));
