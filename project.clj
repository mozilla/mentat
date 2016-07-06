(defproject lib "0.1.0-SNAPSHOT"
  :description "A persistent, embedded knowledge base inspired by Datomic and DataScript."
  :url "https://github.com/mozilla/datomish"
  :license {:name "Mozilla Public License Version 2.0"
            :url "https://github.com/mozilla/datomish/blob/master/LICENSE"}
  :dependencies [[org.clojure/clojurescript "1.9.89"]
                 [org.clojure/clojure "1.8.0"]]

  :cljsbuild {:builds [{:id             "release"
                        :source-paths   ["src"]
                        :assert         false
                        :compiler       {:output-to      "release-js/datomish.bare.js"
                                         :optimizations  :advanced
                                         :pretty-print   false
                                         :elide-asserts  true
                                         :output-wrapper false
                                         :parallel-build true}
                        :notify-command ["release-js/wrap_bare.sh"]}
                       ]}

  :profiles {:dev {:dependencies [[com.cemerick/piggieback "0.2.1"]
                                  [org.clojure/tools.nrepl "0.2.10"]]
                   :repl-options {:nrepl-middleware [cemerick.piggieback/wrap-cljs-repl]}
                   :source-paths []
                   :plugins      [[lein-cljsbuild "1.1.2"]]
                   :cljsbuild    {:builds [{:id           "advanced"
                                            :source-paths ["src"]
                                            :compiler     {
                                                           :output-to            "target/datomish.js"
                                                           :optimizations        :advanced
                                                           :source-map           "target/datomish.js.map"
                                                           :pretty-print         true
                                                           :recompile-dependents false
                                                           :parallel-build       true
                                                           }}
                                           ]}
                   }
             }

  :clean-targets ^{:protect false} ["target"
                                    "release-js/datomish.bare.js"
                                    "release-js/datomish.js"]
  )
