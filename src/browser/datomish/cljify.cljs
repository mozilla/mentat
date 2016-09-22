(ns datomish.cljify)

(defn cljify
  "Does what `(js->clj o :keywordize-keys true) is supposed to do, but works
  in environments with more than one context (e.g., web browsers).

  See <http://dev.clojure.org/jira/browse/CLJS-439?focusedCommentId=43909>.

  Note that Date instances are passed through."
  [o]
  (cond
    (nil? o)
    nil

    ;; Primitives.
    (or
      (true? o)
      (false? o)
      (number? o)
      (string? o)
      ;; Dates are passed through.
      (not (nil? (aget (aget o "__proto__") "getUTCMilliseconds"))))
    o

    ;; Array.
    (.isArray js/Array o)
    (let [n (.-length o)]
      (loop [i 0
             acc (transient [])]
        (if (< i n)
          (recur (inc i) (conj! acc (cljify (aget o i))))
          (persistent! acc))))

    ;; Object.
    (not (nil? (aget (aget o "__proto__") "hasOwnProperty")))
    (let [a (.keys js/Object o)
          n (.-length a)]
      (loop [i 0
             acc (transient {})]
        (if (< i n)
          (let [key (aget a i)]
            (recur (inc i) (assoc! acc
                                   (keyword key)
                                   (cljify (aget o key)))))
          (persistent! acc))))

    :else o))
