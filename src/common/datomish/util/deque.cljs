;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.util.deque)

(defn deque
  "Return a new, empty deque."
  []
  (atom []))

(defn enqueue!
  "Push a value onto the tail of the deque."
  [deque value]
  (swap! deque conj value))

(defn dequeue!
  "Pop a value from the head of the deque.  Returns nil when deque is empty, like first."
  [deque]
  ;; There's only a single thread of control in a JS environment, so there's no possibility that
  ;; this is interrupted and therefore no race between deref (@) and swap!.
  (when-first [head @deque]
    (swap! deque subvec 1)
    head))
