(ns datomish.js
  (:refer-clojure :exclude [])
  (:require
   [datomish.core :as d]
   [cljs.reader]))

;; Public API.

(defn ^:export q [query & sources]
  (let [query   (cljs.reader/read-string query)]
    (clj->js query)))
