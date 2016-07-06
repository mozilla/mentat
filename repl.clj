(require 'cljs.repl)
(require 'cljs.build.api)
(require 'cljs.repl.node)

(cljs.build.api/build
  "src"
  {:main 'datomish.core
   :output-to "target/datomish.js"
   :verbose true})

(cljs.repl/repl
  (cljs.repl.node/repl-env)
  :watch "src"
  :output-dir "target")
