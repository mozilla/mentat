;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

;; A context, very simply, holds on to a default source. Eventually
;; it'll also do projection and similar transforms.
(ns datomish.context)

(defrecord Context [default-source elements cc])
