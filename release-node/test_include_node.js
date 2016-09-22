var d = require('../target/release-node/datomish');
console.log(d.q("[:find ?e ?v :where [?e \"name\" ?v] {:x :y}]"));
