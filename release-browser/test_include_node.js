var d = require('./datomish');
console.log(d.q("[:find ?e ?v :where [?e \"name\" ?v] {:x :y}]"));
