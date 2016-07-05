(require 'cljs.build.api)

(cljs.build.api/build "src"
  {:main 'datomish.core
   :output-to "main.js"
   :target :nodejs})
