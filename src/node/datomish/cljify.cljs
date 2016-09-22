(ns datomish.cljify)

(defn cljify
  "In node, equivalent to `(js->clj o :keywordize-keys true).
  See <http://dev.clojure.org/jira/browse/CLJS-439?focusedCommentId=43909>."
  [o]
  (js->clj o :keywordize-keys true))
