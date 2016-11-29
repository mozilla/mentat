(defproject mozilla/datomish "0.3.5"
  :description "A persistent, embedded knowledge base inspired by Datomic and DataScript."
  :url "https://github.com/mozilla/datomish"
  :license {:name "Apache License, Version 2.0"
            :url "https://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[org.clojure/clojurescript "1.9.229"]
                 [org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.2.385"]
                 [datascript "0.15.4"]
                 [org.clojars.rnewman/honeysql "0.8.2"]
                 ;[com.taoensso/tufte "1.0.2"]
                 [jamesmacaulay/cljs-promises "0.1.0"]]

  ;; The browser will never require from the .JAR anyway.
  :source-paths [
                 "src/common"
                 ;; Can't be enabled by default: layers on top of cljsbuild!
                 ;; Instead, add the :node profile:
                 ;;   lein with-profile node install
                 ;; "src/node"
                 ]

  :test-paths ["test" "src/helpers"]

  :cljsbuild {:builds
              {
               :release-node
               {
                :source-paths   ["src/common" "src/node"]
                :assert         false
                :compiler
                {
                 ;; :externs specified in deps.cljs.
                 :elide-asserts  true
                 :hashbang       false
                 :language-in    :ecmascript5
                 :language-out   :ecmascript5
                 :optimizations  :advanced
                 :output-dir     "target/release-node"
                 :output-to      "target/release-node/datomish.bare.js"
                 :output-wrapper false
                 :parallel-build true
                 :pretty-print   true
                 :pseudo-names   true
                 :static-fns     true
                 :target         :nodejs
                 }
                :notify-command ["release-node/wrap_bare.sh"]}

               :release-browser
               ;; Release builds for use in Firefox must:
               ;; * Use :optimizations > :none, so that a single file is generated
               ;;   without a need to import Closure's own libs.
               ;; * Be wrapped, so that a CommonJS module is produced.
               ;; * Have a preload script that defines what `println` does.
               ;;
               ;; There's no point in generating a source map -- it'll be wrong
               ;; due to wrapping.
               {
                :source-paths   ["src/common" "src/browser"]
                :assert         false
                :compiler
                {
                 :elide-asserts  true
                 :externs        ["src/browser/externs/datomish.js"]
                 :language-in    :ecmascript5
                 :language-out   :ecmascript5
                 :optimizations  :advanced
                 :output-dir     "target/release-browser"
                 :output-to      "target/release-browser/datomish.bare.js"
                 :output-wrapper false
                 :parallel-build true
                 :preloads       [datomish.preload]
                 :pretty-print   true
                 :pseudo-names   true
                 :static-fns     true
                 }
                :notify-command ["release-browser/wrap_bare.sh"]}

               :test
               {
                :source-paths ["src/common" "src/node" "src/helpers" "test"]
                :compiler
                {
                 :language-in    :ecmascript5
                 :language-out   :ecmascript5
                 :main           datomish.test
                 :optimizations  :none
                 :output-dir     "target/test"
                 :output-to      "target/test/datomish.js"
                 :parallel-build true
                 :source-map     true
                 :target         :nodejs
                 }}
               }}

  :profiles {:node {:source-paths ["src/common" "src/node"]}
             :dev {:dependencies [[cljsbuild "1.1.3"]
                                  [tempfile "0.2.0"]
                                  [com.cemerick/piggieback "0.2.1"]
                                  [org.clojure/tools.nrepl "0.2.10"]
                                  [org.clojure/java.jdbc "0.6.2-alpha3"]
                                  [org.xerial/sqlite-jdbc "3.15.1"]]
                   :jvm-opts ["-Xss4m"]
                   :repl-options {:nrepl-middleware [cemerick.piggieback/wrap-cljs-repl]}
                   :plugins      [[lein-cljsbuild "1.1.3"]
                                  [lein-doo "0.1.6"]
                                  [venantius/ultra "0.4.1"]
                                  [com.jakemccrary/lein-test-refresh "0.16.0"]]
                   }}

  :doo {:build "test"}

  :clean-targets ^{:protect false} ["target"]
  )
