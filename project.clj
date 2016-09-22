(defproject datomish "0.1.0-SNAPSHOT"
  :description "A persistent, embedded knowledge base inspired by Datomic and DataScript."
  :url "https://github.com/mozilla/datomish"
  :license {:name "Mozilla Public License Version 2.0"
            :url  "https://github.com/mozilla/datomish/blob/master/LICENSE"}
  :dependencies [[org.clojure/clojurescript "1.9.229"]
                 [org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.2.385"]
                 [datascript "0.15.1"]
                 [honeysql "0.8.0"]
                 [com.taoensso/tufte "1.0.2"]
                 [jamesmacaulay/cljs-promises "0.1.0"]]

  ;; The browser will never require from the .JAR anyway.
  :source-paths [
                 "src/common"
                 ;; Can't be enabled by default: layers on top of cljsbuild!
                 ;; Instead, add the :node profile:
                 ;;   lein with-profile node install
                 ;"src/node"
                 ]

  :cljsbuild {:builds
              {
               :release-node
               {
                :source-paths   ["src/node"]
                :assert         false
                :compiler
                {
                 :elide-asserts  true
                 :hashbang       false
                 :language-in    :ecmascript5
                 :language-out   :ecmascript5
                 :optimizations  :advanced
                 :output-dir     "release-node"
                 :output-to      "release-node/datomish.bare.js"
                 :output-wrapper false
                 :parallel-build true
                 :pretty-print   false
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
                 :externs        ["src/browser/externs.js"]
                 :language-in    :ecmascript5
                 :language-out   :ecmascript5
                 :optimizations  :advanced
                 :output-dir     "release-browser"
                 :output-to      "release-browser/datomish.bare.js"
                 :output-wrapper false
                 :parallel-build true
                 :preloads       [datomish.preload]
                 :pretty-print   true
                 :pseudo-names   true
                 :static-fns     true
                 }
                :notify-command ["release-browser/wrap_bare.sh"]}

               :advanced
               {:source-paths ["src/node"]
                :compiler
                {
                 :language-in    :ecmascript5
                 :language-out   :ecmascript5
                 :output-dir     "target/advanced"
                 :output-to      "target/advanced/datomish.js"
                 :optimizations  :advanced
                 :parallel-build true
                 :pretty-print   true
                 :source-map     "target/advanced/datomish.js.map"
                 :target         :nodejs
                 }}

               :test
               {
                :source-paths ["src/node" "test"]
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
                                  [org.clojure/java.jdbc "0.6.2-alpha1"]
                                  [org.xerial/sqlite-jdbc "3.8.11.2"]]
                   :jvm-opts ["-Xss4m"]
                   :repl-options {:nrepl-middleware [cemerick.piggieback/wrap-cljs-repl]}
                   :plugins      [[lein-cljsbuild "1.1.3"]
                                  [lein-doo "0.1.6"]
                                  [venantius/ultra "0.4.1"]
                                  [com.jakemccrary/lein-test-refresh "0.16.0"]]
                   }}

  :doo {:build "test"}

  :clean-targets ^{:protect false}
  [
   "target"
   "release-node/cljs/"
   "release-node/cljs_promises/"
   "release-node/clojure/"
   "release-node/datascript/"
   "release-node/datomish/"
   "release-node/honeysql/"
   "release-node/taoensso/"
   "release-node/datomish.bare.js"
   "release-node/datomish.js"
   "release-browser/cljs/"
   "release-browser/cljs_promises/"
   "release-browser/clojure/"
   "release-browser/datascript/"
   "release-browser/datomish/"
   "release-browser/honeysql/"
   "release-browser/taoensso/"
   "release-browser/datomish.bare.js"
   "release-browser/datomish.js"
   ]
  )
