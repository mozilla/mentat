/* Copyright 2016 Mozilla
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

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
  let path = "/tmp/testing" + Date.now() + ".db";
  console.log("Opening " + path);
  let db = await datomish.open(path);

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
  console.log("Known URLs: " + JSON.stringify(results));

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
     "page/visitedAt": new Date()}
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

  // Add some more data about a couple of pages.
  let start = Date.now();
  let lr = datomish.tempid();
  let reddit = datomish.tempid();
  let res = await db.transact([
    {"db/id": reddit,
     "page/url": "http://reddit.com/",
     "page/title": "Reddit",
     "page/visitedAt": new Date(start)},
    {"db/id": lr,
     "page/url": "https://longreads.com/",
     "page/title": "Longreads: The best longform stories on the web",
     "page/visitedAt": (new Date(start + 100))},

    // Two visits each.
    {"db/id": lr,
     "page/visitedAt": (new Date(start + 200))},
    {"db/id": reddit,
     "page/visitedAt": (new Date(start + 300))}
  ]);

  // These are our new persistent IDs. We can use these directly in later
  // queries or transactions
  lr = res.tempid(lr);
  reddit = res.tempid(reddit);
  console.log("Persistent IDs are " + lr + ", " + reddit + ".");

  // A query with a limit and order-by. Because we limit to 2, and order
  // by most recent visit date first, we won't get mozilla.org in our results.
  let recent = await db.q(
    `[:find ?url (max ?date)
      :in $
      :where
      [?page :page/url ?url]
      [?page :page/visitedAt ?date]]`,
    {"limit": 2, "order-by": [["_max_date", "desc"]]});

  console.log("Recently visited: " + JSON.stringify(recent));

  // Close: we're done!
  await db.close();
}

testOpen()
.then((r) => console.log("Done."))
.catch((e) => console.log("Failure: " + e.stack));
