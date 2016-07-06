(defproject lib "0.1.0-SNAPSHOT"
  :description "A persistent, embedded knowledge base inspired by Datomic and DataScript."
  :url "https://github.com/mozilla/datomish"
  :license {:name "Mozilla Public License Version 2.0"
            :url "https://github.com/mozilla/datomish/blob/master/LICENSE"}
  :profiles {:dev {:dependencies [[com.cemerick/piggieback "0.2.1"]
                                  [org.clojure/tools.nrepl "0.2.10"]]
                   :repl-options {:nrepl-middleware [cemerick.piggieback/wrap-cljs-repl]}}}
  :dependencies [[org.clojure/clojurescript "1.9.89"]])
