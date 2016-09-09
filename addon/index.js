var self = require("sdk/self");

console.log("Datomish Test");
console.log("This: " + this);

var datomish = require("datomish.js");
datomish.open("/tmp/foobar.db").then(function (db) {
  console.log("Got " + db);
  try {
    db.close();
    console.log("Closed.");
  } catch (e) {
    console.log("Couldn't close: " + e);
  }
});
