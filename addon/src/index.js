/* Any copyright is dedicated to the Public Domain.
   http://creativecommons.org/publicdomain/zero/1.0/ */

var self = require("sdk/self");
var buttons = require('sdk/ui/button/action');
var tabs = require('sdk/tabs');

var datomish = require("datomish.js");

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
     "fulltext":     true,
     "doc":          "A page's title."},
    {"name":         "page/content",
     "type":         "string",
     "cardinality":  "one",          // Simple for now.
     "fulltext":     true,
     "doc":          "A snapshot of the page's content. Should be plain text."},
  ]
};

async function initDB(path) {
  let db = await datomish.open(path);
  await db.ensureSchema(schema);
  return db;
}

async function findURLs(db) {
  let query = `[:find ?page ?url ?title :in $ :where [?page :page/url ?url][(get-else $ ?page :page/title "") ?title]]`;
  let options = new Object();
  options["limit"] = 10;
  return datomish.q(db.db(), query, options);
}

async function findPagesMatching(db, string) {
  let query =
    `[:find ?url ?title
      :in $ ?str
      :where
      [(fulltext $ :any ?str) [[?page]]]
      [?page :page/url ?url]
      [(get-else $ ?page :page/title "") ?title]]`;
  return datomish.q(db.db(), query, {"limit": 10, "inputs": {"str": string}});
}

async function savePage(db, url, title, content) {
  let txResult = await db.transact([{"db/id": 55,
                                     "page/title": title,
                                     "page/url": url
                                     //                                    "page/starred": true
  }]);
  return txResult;
}

async function handleClick(state) {
  let db = await datomish.open("/tmp/testing.db");
  await db.ensureSchema(schema);
  let txResult = await savePage(db, tabs.activeTab.url, tabs.activeTab.title, "Content goes here");
  console.log("Transaction returned " + JSON.stringify(txResult));
  console.log("Transaction instant: " + txResult.txInstant);
  let results = await findURLs(db); //datomish.q(db.db(), "[:find ?url :in $ :where [?e :page/url ?url]]");
  results = results.map(r => r[1]);
  console.log("Query results: " + JSON.stringify(results));
  let pages = await findPagesMatching(db, "goes");
  console.log("Pages: " + JSON.stringify(pages));
  await db.close();
}

/*
async function handleClick(state) {
  console.log("Handling click: " + state);
  let tab = tabs.activeTab;
  console.log("Active tab: " + tab);
  console.log("Active tab: " + tab.url);
  console.log("Active tab: " + tab.title);
  let db = await initDB("/tmp/datomish.db");
  console.log("Opened DB: " + db);
  await savePage(db, tab.url, tab.title, "Content goes here.");
  console.log("Saved page.");

  let urls = await findURLs(db);
  console.log("URLs: " + JSON.stringify(urls));

  let results = await findPagesMatching(db, "goes");
  console.log("Pages: " + JSON.stringify(results));
  await db.close();
}
*/

var button = buttons.ActionButton({
  id: "datomish-save",
  label: "Save Page",
  icon: "./datomish-48.png",
  onClick: handleClick
});
