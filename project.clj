(defproject datomish "0.1.0-SNAPSHOT"
  :description "A persistent, embedded knowledge base inspired by Datomic and DataScript."
  :url "https://github.com/mozilla/datomish"
  :license {:name "Mozilla Public License Version 2.0"
            :url  "https://github.com/mozilla/datomish/blob/master/LICENSE"}
  :dependencies [[org.clojure/clojurescript "1.9.89"]
                 [org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.2.385"]
                 [datascript "0.15.1"]
                 [jamesmacaulay/cljs-promises "0.1.0"]]

  :cljsbuild {:builds {:release {
                                 :source-paths   ["src"]
                                 :assert         false
                                 :compiler       {:output-to      "release-js/datomish.bare.js"
                                                  :optimizations  :advanced
                                                  :pretty-print   false
                                                  :elide-asserts  true
                                                  :output-wrapper false
                                                  :parallel-build true}
                                 :notify-command ["release-js/wrap_bare.sh"]}
                       :advanced {:source-paths ["src"]
                                  :compiler     {:output-to            "target/advanced/datomish.js"
                                                 :optimizations        :advanced
                                                 :source-map           "target/advanced/datomish.js.map"
                                                 :pretty-print         true
                                                 :recompile-dependents false
                                                 :parallel-build       true
                                                 }}
                       :test {
                              :source-paths ["src" "test"]
                              :compiler     {:output-to            "target/test/datomish.js"
                                             :output-dir           "target/test"
                                             :main                 datomish.test
                                             :optimizations        :none
                                             :source-map           true
                                             :recompile-dependents false
                                             :parallel-build       true
                                             :target               :nodejs
                                             }}
                       }
              }

  :profiles {:dev {:dependencies [[com.cemerick/piggieback "0.2.1"]
                                  [org.clojure/tools.nrepl "0.2.10"]
                                  [tempfile "0.2.0"]
                                  [org.clojure/java.jdbc "0.6.2-alpha1"]
                                  [org.xerial/sqlite-jdbc "3.8.11.2"]]
                   :repl-options {:nrepl-middleware [cemerick.piggieback/wrap-cljs-repl]}
                   :plugins      [[lein-cljsbuild "1.1.2"]
                                  [lein-doo "0.1.6"]]
                   }}

  :doo {:build "test"}

  :clean-targets ^{:protect false} ["target"
                                    "release-js/datomish.bare.js"
                                    "release-js/datomish.js"]
  )
