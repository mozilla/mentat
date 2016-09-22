(ns datomish.promises
  #?(:cljs
     (:require-macros
        [datomish.pair-chan :refer [go-pair <?]]))
  (:require
     #?@(:clj [[datomish.pair-chan :refer [go-pair]]
               [clojure.core.async :as a :refer [take!]]])
     #?@(:cljs [[cljs-promises.core :refer [promise]]
                [cljs.core.async :as a :refer [take!]]])))

(defn take-pair-as-promise!
  "Just like take-as-promise!, but aware that it's handling a pair channel.
   Also converts values, if desired."
  ([ch]
   (take-pair-as-promise! ch identity))
  ([ch f]
    (promise
      (fn [resolve reject]
        (take!
          ch
          (fn [[v e]]
            (if e
              (reject e)
              (resolve (f v)))))))))

(defmacro go-promise [f & body]
  `(datomish.promises/take-pair-as-promise!
     (datomish.pair-chan/go-pair
       ~@body)
     ~f))
