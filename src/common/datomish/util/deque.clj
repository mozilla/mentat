;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.util.deque)

(defn deque
  "Return a new, empty deque."
  []
  (ref []))

(defn enqueue!
  "Push a value onto the tail of the deque."
  [deque value]
  (dosync
    (alter deque conj value)))

(defn dequeue!
  "Pop a value from the head of the deque.  Returns nil when deque is empty, like first."
  [deque]
  (dosync
    (when-first [head @deque]
      (alter deque subvec 1)
      head)))
