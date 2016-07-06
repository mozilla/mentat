#!/bin/sh

set -e

(cat release-js/wrapper.prefix; cat release-js/datomish.bare.js; cat release-js/wrapper.suffix) > release-js/datomish.js

echo "Packed release-js/datomish.js"
