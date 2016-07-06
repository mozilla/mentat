(ns datomish.util
  (:require
     [clojure.string :as str]))

(defn raise [s]
  #?(:clj (throw (Exception. s)))
  #?(:cljs (throw (js/Error s))))

(defn var->sql-var
  "Turns '?xyz into :xyz."
  [x]
  (when-not (and (symbol? x)
                 (str/starts-with? (name x) "?"))
    (raise (str x " is not a Datalog var.")))
  (keyword (subs (name x) 1)))
